#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/t3/stage/T3BaseStager.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 17.04.2021
# Last Modified Date: 23.04.2021
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from itertools import islice
from contextlib import contextmanager
from multiprocessing import JoinableQueue
from multiprocessing.pool import ThreadPool, AsyncResult
from typing import Union, Optional, Set, Tuple, Type, List, Iterable, Dict, Generator, Any

from ampel.type import StockId, ChannelId, Tag, ubson
from ampel.type.composed import T3Result
from ampel.log.utils import report_exception
from ampel.model.UnitModel import UnitModel
from ampel.abstract.AbsT3Unit import AbsT3Unit, T
from ampel.log import VERBOSE, AmpelLogger, LogFlag
from ampel.log.handlers.ChanRecordBufHandler import ChanRecordBufHandler
from ampel.log.handlers.DefaultRecordBufferingHandler import DefaultRecordBufferingHandler
from ampel.t3.stage.AbsT3Stager import AbsT3Stager
from ampel.view.SnapView import SnapView
from ampel.content.T3Record import T3Record
from ampel.enum.DocumentCode import DocumentCode
from ampel.enum.T3RecordCode import T3RecordCode
from ampel.struct.AmpelBuffer import AmpelBuffer
from ampel.struct.JournalTweak import JournalTweak
from ampel.struct.DocAttributes import DocAttributes
from ampel.util.freeze import recursive_freeze
from ampel.t3.stage.BaseViewGenerator import BaseViewGenerator
from ampel.t3.stage.ThreadedViewGenerator import ThreadedViewGenerator


class T3BaseStager(AbsT3Stager, abstract=True):
	"""
	Supply stock views to one or more T3 units.
	"""

	def __init__(self, **kwargs) -> None:

		super().__init__(**kwargs)
		self.t3_records: List[T3Record] = []
		self.t3_doc_codes: Set[int] = set()
		self.t3_doc_tags: Set[Tag] = set()


	# mandatory
	def get_tags(self) -> Optional[List[Tag]]:
		""" Return collected T3Document tags """
		return list(self.t3_doc_tags)


	# mandatory
	def get_codes(self) -> Union[int, List[int]]:
		""" Return collected T3Document codes """
		return list(self.t3_doc_codes) if self.t3_doc_codes else DocumentCode.OK


	def get_unit(self, um: UnitModel, chan: Optional[ChannelId] = None) -> AbsT3Unit:
		"""
		Returns T3 unit instance and associated view (parametrized via generic type)
		"""

		if self.logger.verbose:
			self.logger.log(VERBOSE, f"Instantiating unit {um.unit}")

		# Spawn unit instance
		return self.context.loader.new_base_unit(
			unit_model = um,
			logger = AmpelLogger.get_logger(
				base_flag = (getattr(self.logger, 'base_flag', 0) & ~LogFlag.CORE) | LogFlag.UNIT,
				console = False,
				handlers = [
					ChanRecordBufHandler(self.logger.level, self.channel or chan) # type: ignore[arg-type] # too much for mypy
					if (self.channel or chan) else DefaultRecordBufferingHandler(self.logger.level)
				]
			),
			sub_type = AbsT3Unit,
			session_info = self.session_info
		)


	def supply(self, t3_unit: AbsT3Unit, view_generator: BaseViewGenerator[T]) -> Optional[T3Record]:
		"""
		Supplies T3 unit with provided generator of views and handle the result
		"""

		try:
			# potential T3Record to be included in the T3Document
			if (ret := t3_unit.process(view_generator)):
				return self.handle_t3_result(t3_unit, ret, view_generator.stocks)

			return None

		except Exception as e:
			self.handle_error(e)
		finally:
			self.flush(t3_unit)

		return None


	def multi_supply(self,
		t3_units: List[AbsT3Unit],
		buf_gen: Generator[AmpelBuffer, None, None]
	) -> Optional[List[T3Record]]:
		"""
		Supplies T3 units with provided views crafted using the provided buffer generator and handle t3 results.
		Note: code in here is not optimized for compactness but for execution speed
		"""

		try:

			# Create and start T3 units "process(...)" threads (generator will block)
			with ThreadPool(processes=len(t3_units)) as pool:

				queues, generators, async_results = self.create_threaded_generators(pool, t3_units)

				# Optimize by potentially grouping units associated with the same view type
				qdict: Dict[Type, List[JoinableQueue]] = {}
				for unit in t3_units:
					if unit.__class__._View not in qdict:
						qdict[unit.__class__._View] = []
					qdict[unit.__class__._View].append(queues[unit])

				# Potentially chunk (and join) to ensure that t3 units process views at a similar pace
				with self.chunk_buffers(buf_gen, queues.values()) as buffers:
					self.put_views(buffers, qdict)

				# Send sentinel to threaded view generators
				for q in queues.values():
					q.put(None) # type: ignore[arg-type]

				# Collect potential unit results
				ret: List[T3Record] = []
				for async_res, generator, t3_unit in zip(async_results, generators, t3_units):

					# potential T3Record to be included in the T3Document
					if (t3_unit_result := async_res.get()):
						if (x := self.handle_t3_result(t3_unit, t3_unit_result, generator.stocks)):
							ret.append(x)

			self.flush(t3_units)
			return ret

		except Exception as e:
			self.flush(t3_units)
			self.handle_error(e)

		return None


	def create_threaded_generators(self, pool: ThreadPool, t3_units: List[AbsT3Unit]) -> Tuple[
		Dict[AbsT3Unit, "JoinableQueue[SnapView]"],
		List[ThreadedViewGenerator],
		List[AsyncResult]
	]:
		"""
		Create and start T3 units "process(...)" threads (generator will block)
		"""

		queues: Dict[AbsT3Unit, JoinableQueue[SnapView]] = {}
		generators: List[ThreadedViewGenerator] = []
		async_results: List[AsyncResult] = []

		for t3_unit in t3_units:
			queues[t3_unit] = JoinableQueue()
			generators.append(
				ThreadedViewGenerator(
					t3_unit.__class__.__name__, queues[t3_unit], self.jupdater
				)
			)
			async_results.append(
				pool.apply_async(t3_unit.process, args=(generators[-1], ))
			)

		return queues, generators, async_results


	def put_views(self, buffers: Iterable[AmpelBuffer], qdict: Dict[Type, List[JoinableQueue]]) -> None:
		"""
		Note: code in here is not optimized for compactness but for execution speed
		"""

		# Simple case: all t3 units are associated with the same type of view
		if len(qdict) == 1:

			View = next(iter(qdict.keys()))
			qs = next(iter(qdict.values()))

			# In paranoia mode, we create a new view from the same buffer for each t3 unit
			if self.paranoia:
				for ab in buffers:
					for q in qs:
						q.put(View(**recursive_freeze(ab)))
			else:
				for ab in buffers:
					v = View(**recursive_freeze(ab))
					for q in qs:
						q.put(v)

		# t3 units are associated with different type of view
		else:

			# Paranoia or say two units == two view types (non-optimizable)
			itms = qdict.items()
			if self.paranoia or all(len(x) == 1 for x in qdict.values()):
				for ab in buffers:
					for View, qs in itms:
						for q in qs:
							q.put(View(**recursive_freeze(ab)))

			# Optimize by potentially grouping units associated with the same view type
			else:
				for ab in buffers:
					for View, qs in itms:
						view = View(**recursive_freeze(ab))
						for q in qs:
							q.put(view)


	@contextmanager
	def chunk_buffers(self, gen: Generator, queues: Iterable[JoinableQueue]):
		"""
		Potentially chunk (and join) to ensure that t3 units process views at a similar pace.
		Allows to keep control of memory footprint by enforcing a maximal number of concurent views.
		"""

		while (buffers := list(islice(gen, self.chunk_size)) if self.chunk_size else gen):

			yield buffers

			# Join view queues before possibly processing next chunk
			for q in queues:
				q.join()


	def handle_error(self, e: Exception) -> None:

		if self.raise_exc:
			raise e

		# Try to insert doc into trouble collection (raises no exception)
		report_exception(
			self.context.db, self.logger, exc=e,
			process=self.jupdater.process_name
		)


	def handle_t3_result(self,
		t3_unit: AbsT3Unit,
		res: Union[T3Result, JournalTweak, Tuple[T3Result, JournalTweak]],
		stocks: List[StockId]
	) -> Optional[T3Record]:

		jt: Optional[JournalTweak] = None
		if isinstance(res, tuple):
			jt = res[1] # type: ignore
			res = res[0]

		elif isinstance(res, JournalTweak):
			jt = res
			res = None

		if jt:
			self.jupdater.add_record(
				stock = stocks, # used to match stock docs
				jtweak = jt,
				unit = t3_unit.__class__.__name__
			)

		if res:
			return self.craft_t3_record(t3_unit, res)

		return None


	def flush(self, arg: Union[AbsT3Unit, Iterable[AbsT3Unit]], extra: Optional[Dict[str, Any]] = None) -> None:

		for t3_unit in [arg] if isinstance(arg, AbsT3Unit) else arg:

			if t3_unit.logger.handlers[0].buffer: # type: ignore[attr-defined]
				t3_unit.logger.handlers[0].forward(self.logger, extra=extra) # type: ignore[attr-defined]
				self.logger.break_aggregation()

			if self.jupdater.update_journal:
				self.jupdater.flush()


	def craft_t3_record(self, t3_unit: AbsT3Unit, res: T3Result) -> T3Record:

		unit_name = t3_unit.__class__.__name__
		rec = T3Record(unit = unit_name)

		if self.channel:
			rec['channel'] = self.channel

		self.t3_records.append(rec)

		if isinstance(res, T3RecordCode):
			rec['code'] = res
		elif isinstance(res, DocAttributes):
			if res.data:
				rec['data'] = res.data
			if res.rec_code:
				rec['code'] = res.rec_code
			if res.doc_code:
				self.t3_doc_codes.add(res.rec_code)
			if res.doc_tag:
				if isinstance(res.doc_tag, list):
					self.t3_doc_tags.update(res.doc_tag)
				else:
					self.t3_doc_tags.add(res.doc_tag)
		# bson
		elif isinstance(res, ubson):
			rec['data'] = res
		else:
			self.logger.error(f"Unsupported type returned by {unit_name}: {type(res)}")

		return rec
