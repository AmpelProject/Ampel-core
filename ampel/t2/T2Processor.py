#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/t2/T2Processor.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 24.05.2019
# Last Modified Date: 04.06.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import gc
import signal
from time import time
from typing import Optional, List, Union, Type, Dict, TypedDict, ItemsView, Any, Sequence
from collections.abc import ItemsView as ColItemsView

from ampel.type import T2UnitResult, ChannelId, Tag
from ampel.util.collections import try_reduce
from ampel.util.t1 import get_datapoint_ids
from ampel.t2.T2RunState import T2RunState
from ampel.content.DataPoint import DataPoint
from ampel.content.Compound import Compound
from ampel.content.T2Record import T2Record
from ampel.content.T2SubRecord import T2SubRecord
from ampel.content.JournalRecord import JournalRecord
from ampel.log import AmpelLogger, DBEventDoc, LogRecordFlag, VERBOSE
from ampel.log.utils import report_exception, report_error
from ampel.log.handlers.DefaultRecordBufferingHandler import DefaultRecordBufferingHandler
from ampel.util.collections import ampel_iter
from ampel.util.mappings import build_unsafe_short_dict_id
from ampel.abstract.AbsProcessorUnit import AbsProcessorUnit
from ampel.abstract.AbsStockT2Unit import AbsStockT2Unit
from ampel.abstract.AbsPointT2Unit import AbsPointT2Unit
from ampel.abstract.AbsStateT2Unit import AbsStateT2Unit
from ampel.abstract.AbsCustomStateT2Unit import AbsCustomStateT2Unit
from ampel.model.UnitModel import UnitModel
from ampel.core.JournalUpdater import JournalUpdater
from ampel.struct.JournalExtra import JournalExtra

AbsT2s = Union[AbsCustomStateT2Unit, AbsStateT2Unit, AbsStockT2Unit, AbsPointT2Unit]

class T2UnitDependency(TypedDict):
	unit: Union[int, str]
	config: int


class T2Processor(AbsProcessorUnit):
	"""
	:param t2_units: ids of the t2 units to run. If not specified, any t2 unit will be run
	:param run_state: only t2 docs with field 'status' matching with provided integer number will be processed
	:param doc_limit: max number of t2 docs to process in run loop
	:param send_beacon: whether to update the beacon collection before run() is executed
	:param gc_collect: whether to actively perform garbage collection between processing of T2 docs

	:param log_profile: See AbsProcessorUnit docstring
	:param db_handler_kwargs: See AbsProcessorUnit docstring
	:param base_log_flag: See AbsProcessorUnit docstring
	:param raise_exc: See AbsProcessorUnit docstring (default False)
	"""

	t2_units: Optional[List[str]]
	run_state: T2RunState = T2RunState.TO_RUN
	doc_limit: Optional[int]
	stock_jtag: Optional[Union[Tag, Sequence[Tag]]]
	send_beacon: bool = True
	gc_collect: bool = True


	def __init__(self, **kwargs) -> None:

		super().__init__(**kwargs)
		self._ampel_db = self.context.db
		self._loader = self.context.loader

		# Ampel hashes unit class names in prod environment
		self.hashes: Dict[int, str] = {}
		self.optimize = self.context.config.get('general.optimize', int)

		if self.optimize and self.optimize > 1:
			for k, d in self.context.config.get('t2.unit.base', list): # type: ignore[union-attr]
				self.hashes[d['hash']] = k

		# Prepare DB query dict
		self.query: Dict[str, Any] = {'status': self.run_state}

		# Possibly restrict query to specified t2 units
		if self.t2_units:
			if len(self.t2_units) == 1:
				self.query['unit'] = self.t2_units[0]
			else:
				self.query['unit'] = {'$in': self.t2_units}

		# Shortcut
		self.col_t0 = self._ampel_db.get_collection('t0')
		self.col_t1 = self._ampel_db.get_collection('t1')
		self.col_t2 = self._ampel_db.get_collection('t2')

		if self.send_beacon:
			self.create_beacon()

		self._run = True
		signal.signal(signal.SIGTERM, self.sig_exit)
		signal.signal(signal.SIGINT, self.sig_exit)


	def sig_exit(self, signum: int, frame) -> None:
		""" Executed when SIGTERM/SIGINT is caught. Stops doc processing in run() """
		self._run = False


	def create_beacon(self) -> None:
		""" Creates a beacon document if it does not exist yet """

		args = {
			'class': self.__class__.__name__,
			't2_units': self.t2_units,
			'run_state': self.run_state,
			'base_log_flag': self.base_log_flag.__int__(),
			'doc_limit': self.doc_limit
		}

		self.beacon_id = build_unsafe_short_dict_id(args)

		# Create beacon doc if it does not exist yet
		self.col_beacon = self._ampel_db.get_collection('beacon')

		# Create specific beacon doc if it does not exist yet
		self.col_beacon.update_one(
			{'_id': self.beacon_id},
			{
				'$setOnInsert': {
					'class': args.pop('class'),
					'config': args
				},
				'$set': {'timestamp': int(time())}
			},
			upsert=True
		)


	def run(self, pre_check: bool = True) -> int:
		""" :returns: number of t2 docs processed """

		# Add new doc in the 'events' collection
		event_doc = DBEventDoc(
			self._ampel_db, process_name=self.process_name, tier=2
		)

		# Avoid 'burning' a run_id for nothing (at the cost of a request)
		if pre_check and self.col_t2.find(self.query).count() == 0:
			return 0

		run_id = self.new_run_id()

		logger = AmpelLogger.from_profile(
			self.context, self.log_profile, run_id,
			base_flag = LogRecordFlag.T2 | LogRecordFlag.CORE | self.base_log_flag
		)

		if self.send_beacon:
			self.col_beacon.update_one(
				{'_id': self.beacon_id},
				{'$set': {'timestamp': int(time())}}
			)

		jupdater = JournalUpdater(
			ampel_db = self.context.db, tier = 2, run_id = run_id,
			process_name = self.process_name, logger = logger,
			raise_exc = self.raise_exc, extra_tag = self.stock_jtag
		)

		# we instantiate t2 unit only once per check interval.
		# t2_instances stores unit instances so that they can be re-used in the loop below
		# Key: run config id(unit name + '_' + run_config name), Value: unit instance
		self.t2_instances: Dict[str, AbsT2s] = {}

		# Loop variables
		counter = 0
		update = {'$set': {'status': T2RunState.RUNNING}}
		doc_limit = self.doc_limit
		gc_collect = self.gc_collect
		update_records = self.update_records
		push_t2_update = self.push_t2_update

		# Process t2 docs until next() returns None (breaks condition below)
		self._run = True
		while self._run:

			# get t2 document (status is usually TO_RUN or TO_RUN_PRIO)
			t2_doc = self.col_t2.find_one_and_update(self.query, update)

			# Cursor exhausted
			if t2_doc is None:
				break
			elif logger.verbose > 1:
				logger.debug(f"T2 doc: {t2_doc}")

			counter += 1
			before_run = time()

			t2_unit = self.get_unit_instance(t2_doc, logger)
			ret = self.process_t2_doc(t2_unit, t2_doc, logger)

			# Used as timestamp and to compute duration below (using before_run)
			now = time()

			try:

				# New journal entry for the associated stock document
				jrec = jupdater.add_record(
					stock = t2_doc['stock'],
					doc_id = t2_doc['_id'],
					unit = t2_doc['unit'],
					channel = try_reduce(t2_doc['channel'])
				)

				# New t2 sub-result entry (later appended with additional values)
				sub_rec: T2SubRecord = {
					'ts': jrec['ts'],
					'run': run_id,
					'duration': round(now - before_run, 3)
				}

				if v := t2_unit.get_version():
					sub_rec['version'] = v

				# T2 units can return a T2RunState integer rather than a dict instance / tuple
				# for example: T2RunState.EXCEPTION, T2RunState.BAD_CONFIG, ...
				if isinstance(ret, int):
					logger.error(f'T2 unit returned int {ret}')
					sub_rec['error'] = ret
					jrec['status'] = ret
					push_t2_update(t2_doc, sub_rec, logger, status=ret)

				# Unit returned just a dict result
				elif isinstance(ret, dict):

					sub_rec['result'] = ret
					jrec['status'] = 0
					push_t2_update(t2_doc, sub_rec, logger)

					# Empty dict, should not happen
					if not ret:
						logger.warn(
							"T2 unit return empty content",
							extra={'t2_doc': t2_doc.pop}
						)

				# Custom JournalExtra returned by unit
				elif isinstance(ret, tuple):

					jrec['status'] = 0
					update_records(sub_rec, jrec, ret[0], ret[1])
					push_t2_update(t2_doc, sub_rec, logger)

					# Empty dict, should not happen
					if not ret:
						logger.warn(
							"T2 unit return empty content",
							extra={'t2_doc': t2_doc.pop}
						)

				# 'tied' t2 unit, can have different results depending on the channel(s) considered
				elif isinstance(ret, ColItemsView):

					for chans, unit_result in ret:

						sub_rec_copy = sub_rec.copy()

						# Error code returned by unit
						if isinstance(unit_result, int):
							sub_rec_copy['error'] = unit_result
							push_t2_update(t2_doc, sub_rec_copy, logger, status=unit_result)

						# No custom JournalExtra returned by unit
						elif isinstance(unit_result, dict):
							jrec_copy = jrec.copy()
							jrec_copy['status'] = 0
							sub_rec_copy['channel'] = chans
							push_t2_update(t2_doc, sub_rec_copy, logger)

						# Custom JournalExtra returned by unit
						elif isinstance(unit_result, tuple):
							jrec_copy = jrec.copy()
							sub_rec_copy['channel'] = chans
							jrec_copy['channel'] = chans
							jrec_copy['status'] = 0
							update_records(sub_rec_copy, jrec_copy, unit_result[0], unit_result[1])
							push_t2_update(t2_doc, sub_rec_copy, logger)

						else:
							self._unsupported_result(unit_result, t2_doc, sub_rec, jrec, logger)

				# Unsupported object returned by unit
				else:
					self._unsupported_result(ret, t2_doc, sub_rec, jrec, logger)

				# Update stock document
				jupdater.flush()

			except Exception as e:
				if self.raise_exc:
					raise e
				self._processing_error(
					logger, t2_rec=t2_doc, sub_rec=sub_rec, jrec=jrec,
					exception=e, sub_rec_msg='An exception occured'
				)

			# Check possibly defined doc_limit
			if doc_limit and counter >= doc_limit:
				break

			if gc_collect:
				gc.collect()


		event_doc.update(logger, docs=counter, run=run_id)

		logger.flush()
		self.t2_instances = {}
		return counter


	def process_t2_doc(self,
		t2_unit: AbsT2s, t2_doc: T2Record, logger: AmpelLogger,
	) -> Union[T2UnitResult, ItemsView[ChannelId, T2UnitResult]]:
		"""
		Regarding the possible int return code:
		usually, if an int is returned, it should be a T2RunState member
		but let's not be too restrictive here
		"""

		try:

			# T2 units bound to states requires loading of compound doc and datapoints
			if isinstance(t2_unit, (AbsStateT2Unit, AbsCustomStateT2Unit)):

				datapoints: List[DataPoint] = []
				link = t2_doc['link'][0] if isinstance(t2_doc['link'], list) else t2_doc['link']
				compound: Optional[Compound] = next(self.col_t1.find({'_id': link}), None)

				# compound doc must exist (None could mean an ingester bug)
				if compound is None:
					report_error(
						self._ampel_db, msg='Compound not found', logger=logger,
						info={'id': t2_doc['link'], 'doc': t2_doc}
					)
					return T2RunState.ERROR

				# Datarights: suppress channel info (T3 uses instead a
				# 'projection' technic that should not be necessary here)
				compound.pop('channel')

				dps_ids = get_datapoint_ids(compound, logger)
				datapoints = list(self.col_t0.find({'_id': {"$in": dps_ids}}))

				if not datapoints:
					report_error(
						self._ampel_db, msg='Datapoints not found', logger=logger,
						info={'id': compound, 'doc': t2_doc}
					)
					return T2RunState.ERROR

				elif len(datapoints) != len(dps_ids):
					for el in set(dps_ids) - {el['_id'] for el in datapoints}:
						logger.error(f"Datapoint {el} referenced in compound not found")
					return T2RunState.ERROR

				for dp in datapoints:
					dp.pop('excl', None)
					dp.pop('extra', None)
					dp.pop('policy', None)

				if isinstance(t2_unit, AbsStateT2Unit):
					args = [compound, datapoints]
				else: # instance of AbsCustomStateT2Unit
					args = [t2_unit.build(compound, datapoints)]

				if hasattr(t2_unit, 'dependency'):

					t2_records: List[T2Record] = []
					args.append(t2_records)

					for dep in ampel_iter(getattr(t2_unit, 'dependency')):

						t2_info = self.context.config.get(
							f't2.unit.base.{dep["unit"]}', dict
						)

						if not t2_info:
							raise ValueError(f'Unknown T2 unit {dep["unit"]}')

						for dep_t2_doc in self.col_t2.find(
							{
								'unit': dep['unit'],
								'config': dep['config'],
								'stock': t2_doc['stock'],
								'channel': {'$in': t2_doc['channel']},
								'link': t2_doc[
									'stock' if 'AbsStockT2Unit' in t2_info['base'] # type: ignore
									else 'link'
								]
							}
						):
							# suppress channel info
							dep_t2_doc.pop('channel')
							t2_records.append(dep_t2_doc)

				ret = t2_unit.run(*args) # type: ignore[arg-type]

				if t2_unit._buf_hdlr.buffer: # type: ignore[union-attr]
					t2_unit._buf_hdlr.forward( # type: ignore[union-attr]
						logger, stock=t2_doc['stock'], channel=t2_doc['channel']
					)

				return ret

			elif isinstance(t2_unit, (AbsStockT2Unit, AbsPointT2Unit)):

				if doc := next(self.col_t0.find({'_id': t2_doc['link']}), None):
					ret = t2_unit.run(doc)
					if t2_unit._buf_hdlr.buffer: # type: ignore[union-attr]
						t2_unit._buf_hdlr.forward( # type: ignore[union-attr]
							logger, stock=t2_doc['stock'], channel=t2_doc['channel']
						)
					return ret

				report_error(
					self._ampel_db, msg='Datapoint not found' if isinstance(t2_unit, AbsPointT2Unit)
					else 'Stock doc not found', logger=logger, info={'doc': t2_doc}
				)

				return T2RunState.ERROR

			else:

				report_error(
					self._ampel_db, msg='Unknown T2 unit type',
					logger=logger, info={'doc': t2_doc}
				)

				return T2RunState.ERROR

		except Exception as e:
			if self.raise_exc:
				raise e
			# Record any uncaught exceptions in troubles collection.
			report_exception(
				self._ampel_db, logger, exc=e, info={
					'_id': t2_doc['_id'],
					'unit': t2_doc['unit'],
					'config': t2_doc['config'],
					'link': t2_doc['link']
				}
			)

			return T2RunState.EXCEPTION


	def _unsupported_result(self,
		unit_ret: Any, t2_rec: T2Record, sub_rec: T2SubRecord,
		jrec: JournalRecord, logger: AmpelLogger
	) -> None:

		self._processing_error(
			logger, t2_rec=t2_rec, sub_rec=sub_rec, jrec=jrec,
			sub_rec_msg='Unit returned invalid content',
			report_msg='Invalid T2 unit return code',
			extra={'ret': unit_ret}
		)


	def _processing_error(self,
		logger: AmpelLogger, t2_rec: T2Record, sub_rec: T2SubRecord,
		jrec: JournalRecord, sub_rec_msg: str, report_msg: Optional[str] = None,
		extra: Dict[str, Any] = {}, exception: Optional[Exception] = None
	) -> None:
		"""
		- Updates the t2 document by appending a T2SubRecord and
		updating 'status' with value T2RunState.EXCEPTION if an exception
		if provided through the parameter 'exception' or with value
		T2RunState.ERROR otherwise. The field  'msg' of the created
		T2SubRecord entry will be set to the value of parameter 'sub_rec_msg'.

		- Updates the stock document by appending a JournalRecord to it
		and by updating the 'modified' timestamp

		- Creates a 'trouble' document in the troubles collection
		to report the incident
		"""

		sub_rec = sub_rec.copy()
		sub_rec['msg'] = sub_rec_msg

		self.push_t2_update(
			t2_rec, sub_rec, logger,
			status = T2RunState.EXCEPTION if exception else T2RunState.ERROR
		)

		info: Dict[str, Any] = {
			**extra,
			'run': sub_rec['run'],
			'stock': t2_rec['stock'],
			'doc': t2_rec
		}

		if exception:
			report_exception(
				self._ampel_db, logger=logger,
				exc=exception, info=info
			)
		else:
			report_error(
				self._ampel_db, logger=logger,
				msg=report_msg if report_msg else '', info=info
			)


	def update_records(self,
		sub_rec: T2SubRecord, jrec: JournalRecord,
		result: Dict[str, Any], jextra: JournalExtra
	) -> None:
		"""
		Updates provided T2SubRecord and JournalRecord
		using results of a t2 unit (Tuple[Dict, JournalExtra])
		"""

		sub_rec['result'] = result

		if jextra.tag:
			sub_rec['jup'] = True
			JournalUpdater.include_tags(jrec, jextra.tag)

		if jextra.status:
			sub_rec['jup'] = True
			jrec['status'] = jextra.status

		if jextra.extra:
			sub_rec['jup'] = True
			jrec['extra'] = jextra.extra


	def push_t2_update(self,
		rec: T2Record, sub_rec: T2SubRecord, logger: AmpelLogger,
		*, status: int = T2RunState.COMPLETED
	) -> None:
		""" Performs DB updates of the T2 doc and stock journal """

		if logger.verbose:
			logger.log(VERBOSE, 'Saving T2 unit result')

		# Update T2 document
		self.col_t2.update_one(
			{'_id': rec['_id']},
			{
				'$push': {'body': sub_rec},
				'$set': {'status': status}
			}
		)


	def get_unit_instance(self, t2_doc: T2Record, logger: AmpelLogger) -> AbsT2s:

		k = f'{t2_doc["unit"]}_{t2_doc["config"]}'

		# Check if T2 instance exists in this run
		if k not in self.t2_instances:

			if 'col' in t2_doc:
				if 't0' in t2_doc['col']: # type: ignore[operator]
					sub_type: Type[AbsT2s] = AbsPointT2Unit
				elif 'stock' in t2_doc['col']: # type: ignore[operator]
					sub_type = AbsStockT2Unit
				else:
					raise ValueError('Unsupported t2 unit type')
			else:
				bcs = self.context.config.get(f"unit.base.{t2_doc['unit']}.base", (list, tuple)) # type: ignore
				if bcs is None:
					raise ValueError(f'Unknown t2 unit: {t2_doc["unit"]}')
				if "AbsStateT2Unit" in bcs:
					sub_type = AbsStateT2Unit
				elif "AbsCustomStateT2Unit" in bcs:
					sub_type = AbsCustomStateT2Unit
				else:
					raise ValueError(f'Unsupported t2 unit (base classes: {bcs})')

			# Create channel (buffering) logger
			buf_hdlr = DefaultRecordBufferingHandler(level=logger.level)
			buf_logger = AmpelLogger.get_logger(
				name = k,
				base_flag = (getattr(logger, 'base_flag', 0) & ~LogRecordFlag.CORE) | LogRecordFlag.UNIT,
				console = False,
				handlers = [buf_hdlr]
			)

			# Instantiate t2 unit
			unit_instance = self._loader.new_base_unit(
				unit_model = UnitModel(
					unit = t2_doc['unit'] if isinstance(t2_doc['unit'], str)
						else self.hashes[t2_doc['unit']],
					config = t2_doc['config']
				),
				logger = buf_logger,
				sub_type = sub_type
			)

			# Shortcut to avoid unit_instance.logger.handlers[?]
			setattr(unit_instance, '_buf_hdlr', buf_hdlr)

			# Check for possibly defined dependencies
			if hasattr(unit_instance, 'dependency'):

				deps: List[T2UnitDependency] = []

				for dep in ampel_iter(getattr(unit_instance, 'dependency')):

					# Replace unit class name with saved hashed value
					if self.optimize and self.optimize > 1:
						unit_info = self.context.config.get('t2.unit.base.{dep["unit"]}', dict)
						if unit_info is None:
							raise ValueError(f'No info found for tied unit {dep["unit"]}')
						dep['unit'] = unit_info['hash']

					deps.append(
						{
							'unit': dep['unit'],
							'config': build_unsafe_short_dict_id(dep.get('config'))
						}
					)

				# "replace" static 'dependency' with instance dependency
				unit_instance.__setattr__('dependency', deps)

			self.t2_instances[k] = unit_instance # type: ignore

		return self.t2_instances[k]
