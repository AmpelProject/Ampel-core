#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/t3/stage/T3BaseStager.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 17.04.2021
# Last Modified Date: 23.04.2021
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from time import time
from itertools import islice
from multiprocessing import JoinableQueue
from multiprocessing.pool import ThreadPool, AsyncResult
from typing import Union, Optional, Tuple, Type, List, Iterable, Dict, Generator, Any

from ampel.types import StockId, ChannelId, UBson, ubson
from ampel.log.utils import report_exception
from ampel.model.UnitModel import UnitModel
from ampel.abstract.AbsT3Unit import AbsT3Unit, T
from ampel.log import VERBOSE, AmpelLogger, LogFlag
from ampel.log.handlers.ChanRecordBufHandler import ChanRecordBufHandler
from ampel.log.handlers.DefaultRecordBufferingHandler import DefaultRecordBufferingHandler
from ampel.abstract.AbsT3Stager import AbsT3Stager
from ampel.view.SnapView import SnapView
from ampel.content.T3Document import T3Document
from ampel.struct.AmpelBuffer import AmpelBuffer
from ampel.struct.UnitResult import UnitResult
from ampel.enum.DocumentCode import DocumentCode
from ampel.t3.stage.BaseViewGenerator import BaseViewGenerator
from ampel.t3.stage.ThreadedViewGenerator import ThreadedViewGenerator
from ampel.util.mappings import dictify
from ampel.util.freeze import recursive_freeze
from ampel.util.hash import build_unsafe_dict_id


class T3BaseStager(AbsT3Stager, abstract=True):
	"""
	Supply stock views to one or more T3 units.
	"""

	save_stock_ids: bool = False

	def get_unit(self, um: UnitModel, chan: Optional[ChannelId] = None) -> AbsT3Unit:
		"""
		Returns T3 unit instance and associated view (parametrized via generic type)
		"""

		if self.logger.verbose:
			self.logger.log(VERBOSE, f"Instantiating unit {um.unit}")
		self.logger.log(VERBOSE, f"session_info unit {self.session_info}")

		c = self.channel or chan

		# Spawn unit instance
		return self.context.loader.new_logical_unit(
			model = um,
			logger = AmpelLogger.get_logger(
				base_flag = (getattr(self.logger, 'base_flag', 0) & ~LogFlag.CORE) | LogFlag.UNIT,
				console = False,
				handlers = [
					ChanRecordBufHandler(self.logger.level, c, {'unit': um.unit}) \
					if c else DefaultRecordBufferingHandler(self.logger.level, {'unit': um.unit})
				]
			),
			sub_type = AbsT3Unit,
			session_info = self.session_info
		)


	def supply(self, t3_unit: AbsT3Unit, view_generator: BaseViewGenerator[T]) -> Optional[T3Document]:
		"""
		Supplies T3 unit with provided generator of views and handle the result
		"""

		ts = time()

		try:

			# potential T3Document to be included in the T3Document
			if (ret := t3_unit.process(view_generator)) or self.save_stock_ids:
				return self.handle_t3_result(t3_unit, ret, view_generator.get_stock_ids(), ts)

			return None

		except Exception as e:
			self.handle_error(e)
		finally:
			self.flush(t3_unit)

		return None


	def multi_supply(self,
		t3_units: List[AbsT3Unit],
		buf_gen: Generator[AmpelBuffer, None, None]
	) -> Optional[List[T3Document]]:
		"""
		Supplies T3 units with provided views crafted using the provided buffer generator and handle t3 results.
		Note: code in here is not optimized for compactness but for execution speed
		"""

		ts = time()

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
				qv = queues.values()

				try:
				
					while (buffers := list(islice(buf_gen, self.chunk_size)) if self.chunk_size else buf_gen):

						self.put_views(buffers, qdict)

						# Join view queues before possibly processing next chunk
						for q in qv:
							q.join()

					# Send sentinel to threaded view generators
					for q in qv:
						q.put(None) # type: ignore[arg-type]

					# Collect potential unit results
					ret: List[T3Document] = []
					for async_res, generator, t3_unit in zip(async_results, generators, t3_units):

						# potential T3Document to be included in the T3Document
						if (t3_unit_result := async_res.get()):
							if (x := self.handle_t3_result(t3_unit, t3_unit_result, generator.stocks, ts)):
								ret.append(x)

				except RuntimeError as e:
					if "StopIteration" in str(e):
						return None
					raise e

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
		res: Union[UBson, UnitResult],
		stocks: List[StockId],
		ts: float
	) -> Optional[T3Document]:

		if isinstance(res, UnitResult):
			if res.journal:
				self.jupdater.add_record(
					stock = stocks, # used to match stock docs
					jattrs = res.journal,
					unit = t3_unit.__class__.__name__
				)
			if res.body is not None or res.code is not None:
				return self.craft_t3_doc(t3_unit, res, ts, stocks)
		elif res is not None or (res is None and self.save_stock_ids and stocks):
			return self.craft_t3_doc(t3_unit, res, ts, stocks)

		return None


	def flush(self, arg: Union[AbsT3Unit, Iterable[AbsT3Unit]], extra: Optional[Dict[str, Any]] = None) -> None:

		for t3_unit in [arg] if isinstance(arg, AbsT3Unit) else arg:

			if t3_unit.logger.handlers[0].buffer: # type: ignore[attr-defined]
				t3_unit.logger.handlers[0].forward(self.logger, extra=extra) # type: ignore[attr-defined]
				self.logger.break_aggregation()

			if self.jupdater.update_journal:
				self.jupdater.flush()


	def craft_t3_doc(self,
		t3_unit: AbsT3Unit,
		res: Union[None, UBson, UnitResult],
		ts: float,
		stocks: Optional[List[StockId]] = None
	) -> T3Document:

		unit_name = t3_unit.__class__.__name__
		confid = build_unsafe_dict_id(tcd := dictify(t3_unit._trace_content))
		self.context.db.add_conf_id(confid, tcd)
		t3d: T3Document = {'unit': unit_name, 'config': confid}

		if self.channel:
			t3d['channel'] = self.channel

		t3d['code'] = DocumentCode.OK
		t3d['meta'] = {'duration': time() - ts}

		if self.save_stock_ids and stocks:
			t3d['stock'] = stocks

		if isinstance(res, UnitResult):
			if res.code:
				t3d['code'] = res.code
			if res.body:
				t3d['body'] = res.body

		# bson
		elif isinstance(res, ubson):
			t3d['body'] = res

		return t3d
