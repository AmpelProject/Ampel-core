#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/t2/T2Processor.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 24.05.2019
# Last Modified Date: 02.04.2021
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import gc
import signal
from time import time
from bson import ObjectId
from typing import Optional, List, Union, Type, Dict, Any, Sequence, Tuple, cast, overload

import backoff

from ampel.type import DataPointId, StockId, T, T2UnitResult, Tag
from ampel.db.query.utils import match_array
from ampel.util.collections import try_reduce
from ampel.log.utils import convert_dollars
from ampel.util.t1 import get_datapoint_ids
from ampel.enum.T2RunState import T2RunState
from ampel.enum.T2SysRunState import T2SysRunState
from ampel.struct.T2BroadUnitResult import T2BroadUnitResult
from ampel.content.StockDocument import StockDocument
from ampel.content.DataPoint import DataPoint
from ampel.content.Compound import Compound
from ampel.content.T2Document import T2Document
from ampel.content.T2Record import T2Record
from ampel.content.JournalRecord import JournalRecord
from ampel.view.T2DocView import T2DocView, TYPE_POINT_T2, TYPE_STOCK_T2, TYPE_STATE_T2
from ampel.log import AmpelLogger, DBEventDoc, LogFlag, VERBOSE
from ampel.base.BadConfig import BadConfig
from ampel.log.utils import report_exception, report_error
from ampel.log.handlers.DefaultRecordBufferingHandler import DefaultRecordBufferingHandler
from ampel.util.mappings import build_unsafe_short_dict_id
from ampel.abstract.AbsProcessorUnit import AbsProcessorUnit
from ampel.abstract.AbsStockT2Unit import AbsStockT2Unit
from ampel.abstract.AbsPointT2Unit import AbsPointT2Unit
from ampel.abstract.AbsStateT2Unit import AbsStateT2Unit
from ampel.abstract.AbsCustomStateT2Unit import AbsCustomStateT2Unit
from ampel.abstract.AbsTiedT2Unit import AbsTiedT2Unit
from ampel.abstract.AbsTiedPointT2Unit import AbsTiedPointT2Unit
from ampel.abstract.AbsTiedStateT2Unit import AbsTiedStateT2Unit
from ampel.abstract.AbsTiedStockT2Unit import AbsTiedStockT2Unit
from ampel.abstract.AbsTiedCustomStateT2Unit import AbsTiedCustomStateT2Unit
from ampel.model.UnitModel import UnitModel
from ampel.model.StateT2Dependency import StateT2Dependency
from ampel.core.JournalUpdater import JournalUpdater
from ampel.metrics.AmpelMetricsRegistry import AmpelMetricsRegistry

AbsT2 = Union[
	AbsStockT2Unit, AbsPointT2Unit, AbsStateT2Unit, AbsTiedPointT2Unit,
	AbsTiedStateT2Unit, AbsCustomStateT2Unit[T], AbsTiedCustomStateT2Unit[T],
]

stat_latency = AmpelMetricsRegistry.histogram(
	"latency",
	"Delay between T2 doc creation and processing",
	subsystem="t2",
	unit="seconds",
	labelnames=("unit", ),
)

stat_count = AmpelMetricsRegistry.counter(
	"docs_processed",
	"Number of T2 documents processed",
	subsystem="t2",
	labelnames=("unit", )
)

class T2Processor(AbsProcessorUnit):
	"""
	Fill results into pending :class:`T2 documents <ampel.content.T2Document.T2Document>`.
	"""

	#: ids of the t2 units to run. If not specified, any t2 unit will be run.
	t2_units: Optional[List[str]]
	#: process only those :class:`T2 documents <ampel.content.T2Document.T2Document>`
	#: with the given :attr:`~ampel.content.T2Document.T2Document.status`
	run_state: Union[T2SysRunState, Sequence[T2SysRunState]] = [
		T2SysRunState.NEW,
		T2SysRunState.NEW_PRIO,
		T2SysRunState.PENDING_DEPENDENCY,
		T2SysRunState.RERUN_REQUESTED
	]
	#: max number of t2 docs to process in :func:`run`
	doc_limit: Optional[int]
	#: tag(s) to add to the stock :class:`~ampel.content.JournalRecord.JournalRecord`
	#: every time a :class:`~ampel.content.T2Document.T2Document` is processed
	stock_jtag: Optional[Union[Tag, Sequence[Tag]]]
	#: create a "beacon" document indicating the last time T2Processor was run
	#: in the current configuration
	send_beacon: bool = True
	#: explicitly call :func:`gc.collect` after every document
	garbage_collect: bool = True
	#: process dependencies of :class:`tied T2 units <ampel.abtract.AbsTiedT2Unit.AbsTiedT2Unit>`
	run_dependent_t2s: bool = False
	#: maximum number of processing attempts per document
	max_try: int = 5


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

		if isinstance(self.run_state, int):
			self.status_match: Union[T2SysRunState, Dict[str, List[T2SysRunState]]] = self.run_state
		elif isinstance(self.run_state, list):
			self.status_match = {"$in": self.run_state}
		else: # Could be a tuple (pymongo requires list)
			self.status_match = {"$in": list(self.run_state)}

		# Prepare DB query dict
		self.query: Dict[str, Any] = {'status': self.status_match}

		# Possibly restrict query to specified t2 units
		if self.t2_units:
			if len(self.t2_units) == 1:
				self.query['unit'] = self.t2_units[0]
			else:
				self.query['unit'] = {"$in": self.t2_units}

		# Shortcut
		self.col_stock = self._ampel_db.get_collection('stock')
		self.col_t0 = self._ampel_db.get_collection('t0')
		self.col_t1 = self._ampel_db.get_collection('t1')
		self.col_t2 = self._ampel_db.get_collection('t2')

		if self.send_beacon:
			self.create_beacon()

		# t2_instances stores unit instances so that they can be re-used in run()
		# Key: run config id(unit name + '_' + run_config name), Value: unit instance
		self.t2_instances: Dict[str, AbsT2] = {}

		self._run = True
		self._doc_counter = 0
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
			'run_state': self.status_match,
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
					'config': convert_dollars(args)
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
			base_flag = LogFlag.T2 | LogFlag.CORE | self.base_log_flag
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

		# Loop variables
		self._doc_counter = 0
		update = {'$set': {'status': T2SysRunState.RUNNING}}
		doc_limit = self.doc_limit
		garbage_collect = self.garbage_collect

		# Process t2 docs until next() returns None (breaks condition below)
		self._run = True
		self._exception = False
		while self._run:

			# get t2 document (status is usually NEW or NEW_PRIO)
			t2_doc = self.col_t2.find_one_and_update(self.query, update)

			# Cursor exhausted
			if t2_doc is None:
				break
			elif logger.verbose > 1:
				logger.debug(f"T2 doc: {t2_doc}")

			self.process_t2_doc(t2_doc, logger, jupdater)

			# Check possibly defined doc_limit
			if doc_limit and self._doc_counter >= doc_limit:
				break

			if garbage_collect:
				gc.collect()

		event_doc.update(logger, docs=self._doc_counter, run=run_id, success=not self._exception)

		logger.flush()
		self.t2_instances.clear()
		return self._doc_counter


	def process_t2_doc(self,
		t2_doc: T2Document, logger: AmpelLogger, jupdater: JournalUpdater
	) -> T2Record: 

		before_run = time()

		t2_unit = self.get_unit_instance(t2_doc, logger)

		try:

			t2_rec: T2Record = {
				'run': jupdater.run_id,
			}
			j_rec: JournalRecord = {}

			if len(t2_doc.get("body", [])) > self.max_try:
				ret: T2UnitResult = T2SysRunState.TOO_MANY_TRIALS
			else:
				ret = self.run_t2_unit(t2_unit, t2_doc, logger, jupdater)

			# Used as timestamp and to compute duration below (using before_run)
			now = time()

			# _id is an ObjectId, but declared as bytes in ampel-interface to avoid
			# an explicit dependency on pymongo
			stat_latency.labels(t2_doc["unit"]).observe(
				now - cast("ObjectId", t2_doc["_id"]).generation_time.timestamp()
			)
			stat_count.labels(t2_doc["unit"]).inc()
			self._doc_counter += 1

			# New journal entry for the associated stock document
			j_rec = jupdater.add_record(
				stock = t2_doc['stock'],
				doc_id = t2_doc['_id'],
				unit = t2_doc['unit'],
				channel = try_reduce(t2_doc['channel'])
			)

			# New t2 sub-result entry (later appended with additional values)
			t2_rec = {
				'ts': j_rec['ts'],
				'run': jupdater.run_id,
				'duration': round(now - before_run, 3)
			}

			tag = None
			if v := t2_unit.get_version():
				t2_rec['version'] = v

			# T2 units can return a T2RunState integer rather than a dict instance / tuple
			# for example: T2RunState.ERROR, T2RunState.PENDING_DEPENDENCY, ...
			if isinstance(ret, int):
				if ret in T2SysRunState.__members__.values():
					logger.info(f'T2Processor status: {ret} ({T2SysRunState(ret).name})')
				else:
					logger.info(f'T2 unit returned int {ret}')
				doc_status = ret
				t2_rec['status'] = ret
				j_rec['status'] = ret

			# Unit returned just a dict result
			elif isinstance(ret, (dict, list)):
				doc_status = T2RunState.COMPLETED
				t2_rec['result'] = ret
				t2_rec['status'] = T2RunState.COMPLETED
				j_rec['status'] = T2RunState.COMPLETED

			# Customizations request by T2 unit
			elif isinstance(ret, T2BroadUnitResult):
				t2_rec['result'] = ret.rec_payload
				t2_rec['status'] = ret.rec_status
				doc_status = ret.doc_status
				tag = ret.doc_tag

				# This just sets a default as journal status can be customized via j_tweak
				j_rec['status'] = T2RunState.COMPLETED

				# Custom JournalTweak returned by unit
				if (j_tweak := ret.stock_journal_tweak) is not None:

					if j_tweak.tag:
						t2_rec['jup'] = True
						JournalUpdater.include_tags(j_rec, j_tweak.tag)

					if j_tweak.status:
						t2_rec['jup'] = True
						j_rec['status'] = j_tweak.status

					if j_tweak.extra:
						t2_rec['jup'] = True
						j_rec['extra'] = j_tweak.extra

			# Unsupported object returned by unit
			else:
				doc_status = t2_rec['status'] = j_rec['status'] = T2RunState.ERROR
				self._processing_error(
					logger, t2_doc=t2_doc, t2_rec=t2_rec, j_rec=j_rec,
					rec_msg='Unit returned invalid content',
					report_msg='Invalid content returned by T2 unit',
					extra={'ret': ret}
				)

			# TODO: check that unit did not use system reserved status

			# Empty payload returned, should not happen
			if 'result' in t2_rec and not t2_rec['result']:
				del t2_rec['result']
				logger.warn(
					"T2 unit return empty content",
					extra={'t2_doc': t2_doc}
				)

			self.push_t2_update(
				t2_doc['_id'], t2_rec, logger,
				tag=tag, status=doc_status
			)

			# Update stock document
			jupdater.flush()

		except Exception as e:
			if self.raise_exc:
				raise e
			self._processing_error(
				logger, t2_doc=t2_doc, t2_rec=t2_rec, j_rec=j_rec,
				exception=e, rec_msg='An exception occured'
			)

		return t2_rec


	@overload
	def load_input_docs(self,
		t2_unit: AbsPointT2Unit,
		t2_doc: T2Document, logger: AmpelLogger, jupdater: JournalUpdater
	) -> Union[None, T2SysRunState, DataPoint]:
		""
		...

	@overload
	def load_input_docs(self,
		t2_unit: AbsTiedPointT2Unit,
		t2_doc: T2Document, logger: AmpelLogger, jupdater: JournalUpdater
	) -> Union[None, T2SysRunState, Tuple[DataPoint, T2DocView]]:
		""
		...

	@overload
	def load_input_docs(self,
		t2_unit: AbsStateT2Unit,
		t2_doc: T2Document, logger: AmpelLogger, jupdater: JournalUpdater,
	) -> Optional[Tuple[Compound, Sequence[DataPoint]]]:
		""
		...


	@overload
	def load_input_docs(self,
		t2_unit: AbsTiedStateT2Unit,
		t2_doc: T2Document, logger: AmpelLogger, jupdater: JournalUpdater
	) -> Union[None, T2SysRunState, Tuple[Compound, Sequence[DataPoint], List[T2DocView]]]:
		""
		...


	@overload
	def load_input_docs(self,
		t2_unit: AbsCustomStateT2Unit[T],
		t2_doc: T2Document, logger: AmpelLogger, jupdater: JournalUpdater
	) -> Optional[Tuple[T]]:
		""
		...


	@overload
	def load_input_docs(self,
		t2_unit: AbsTiedCustomStateT2Unit[T],
		t2_doc: T2Document, logger: AmpelLogger, jupdater: JournalUpdater
	) -> Union[None, T2SysRunState, Tuple[T, List[T2DocView]]]:
		""
		...

	@overload
	def load_input_docs(self,
		t2_unit: AbsStockT2Unit,
		t2_doc: T2Document, logger: AmpelLogger, jupdater: JournalUpdater
	) -> Optional[Tuple["StockDocument"]]:
		...


	# NB: spell out union arg to ensure a common context for the TypeVar T
	def load_input_docs(self,
		t2_unit: AbsT2, t2_doc: T2Document,
		logger: AmpelLogger, jupdater: JournalUpdater
	) -> Union[
		None,
		T2SysRunState,                                         # Error / missing dependency
		DataPoint,                                             # point t2
		Tuple[DataPoint, T2DocView],                           # tied point t2
		Tuple["StockDocument"],                                # stock t2
		Tuple[Compound, Sequence[DataPoint]],                  # state t2
		Tuple[T],                                              # custom state t2 (T could be LightCurve)
		Tuple[Compound, Sequence[DataPoint], List[T2DocView]], # tied state t2
		Tuple[T, List[T2DocView]],                             # tied custom state t2
	]:
		"""
		Fetches documents required by `t2_unit`.

		:param t2_unit: instance to fetch input docs for
		:param t2_doc: document for which inputs are needed
		:param logger: logger
		:param jupdater: journal update service

		Regarding tied T2s (unit depends on other T2s):
		
		- Dependent T2s will be executed if run_dependent_t2s is True.
		- Note that the ingester ingests point and state t2s before state t2s so that the natural ordering
		  of T2 docs to be processed fits rather well a setup with a single T2processor instance processing all T2s.
		- For state t2s tied with other state t2s, putting the dependent units first in the AlertProcessor
		  directives will make sure these are ingested first and thus processed first by T2Processor.
		"""

		# State bound T2 units require loading of compound doc and datapoints
		if isinstance(
			t2_unit, (
				AbsStateT2Unit,
				AbsCustomStateT2Unit,
				AbsTiedStateT2Unit,
				AbsTiedCustomStateT2Unit
			)
		):
			datapoints: List[DataPoint] = []
			# If multiple links are avail, they should be all equivalent
			link = t2_doc['link'][0] if isinstance(t2_doc['link'], list) else t2_doc['link']
			compound: Optional[Compound] = self.load_compound(link)

			# compound doc must exist (None could mean an ingester bug)
			if compound is None:
				report_error(
					self._ampel_db, msg='Compound not found', logger=logger,
					info={'id': t2_doc['link'], 'doc': t2_doc}
				)
				return None

			# Datarights: suppress channel info (T3 uses instead a
			# 'projection' technic that should not be necessary here)
			compound.pop('channel')

			# Datapoints marked as excluded in the compound won't be included
			dps_ids = get_datapoint_ids(compound, logger)
			datapoints = self.load_datapoints(dps_ids)

			if not datapoints:
				report_error(
					self._ampel_db, msg='Datapoints not found', logger=logger,
					info={'id': compound, 'doc': t2_doc}
				)
				return None

			elif len(datapoints) != len(dps_ids):
				for el in set(dps_ids) - {el['_id'] for el in datapoints}:
					logger.error(f"Datapoint {el} referenced in compound not found")
				return None

			for dp in datapoints:
				dp.pop('excl', None) # Channel based exclusion
				dp.pop('extra', None)
				dp.pop('policy', None)

			if isinstance(t2_unit, AbsTiedT2Unit):

				queries: List[Dict[str, Any]] = []

				if isinstance(t2_unit, AbsTiedCustomStateT2Unit):
					# A LightCurve instance for example
					custom_state = t2_unit.build(compound, datapoints)

				for tied_model in t2_unit.get_t2_dependencies():

					d = self.build_tied_t2_query(t2_unit, tied_model, t2_doc)
					if d['link'] is None:

						if isinstance(tied_model, StateT2Dependency) and tied_model.link_override:
							if isinstance(t2_unit, AbsTiedCustomStateT2Unit):
								d['link'] = t2_unit.get_link(tied_model.link_override, custom_state)
							else:
								d['link'] = t2_unit.get_link( # type: ignore[union-attr] # mypy forgot the first "if" of this method
									tied_model.link_override, compound, datapoints
								)
						else:
							d['link'] = match_array(dps_ids)

					queries.append(d)

				tied_views = self.run_tied_queries(queries, t2_doc, jupdater, logger)

				# Dependency missing
				if isinstance(tied_views, T2SysRunState):
					return tied_views

				if isinstance(t2_unit, AbsTiedStateT2Unit):
					return (compound, datapoints, tied_views)

				# instance of AbsTiedCustomStateT2Unit
				return (custom_state, tied_views)

			else:

				if isinstance(t2_unit, AbsStateT2Unit):
					return (compound, datapoints)

				else: # instance of AbsCustomStateT2Unit
					return (t2_unit.build(compound, datapoints), )

		elif isinstance(t2_unit, (AbsStockT2Unit, AbsPointT2Unit, AbsTiedPointT2Unit)):

			if doc := (
				self.load_datapoint(t2_doc['link'])
				if isinstance(t2_unit, AbsPointT2Unit)
				else self.load_stock(t2_doc['link'])
			):
				if isinstance(t2_unit, AbsTiedPointT2Unit):

					tied_views = self.run_tied_queries(
						[
							self.build_tied_t2_query(t2_unit, tied_model, t2_doc)
							for tied_model in t2_unit.get_t2_dependencies()
						],
						t2_doc, jupdater, logger
					)

					if isinstance(tied_views, T2SysRunState):
						return tied_views

					return (doc, tied_views)

				return (doc, )

			report_error(
				self._ampel_db, msg='Datapoint not found' if isinstance(t2_unit, (AbsTiedPointT2Unit, AbsPointT2Unit))
				else 'Stock doc not found', logger=logger, info={'doc': t2_doc}
			)

			return None

		else:

			report_error(
				self._ampel_db, msg='Unknown T2 unit type',
				logger=logger, info={'doc': t2_doc}
			)

			return None


	# allow 10 seconds for input documents to become available
	@backoff.on_predicate(backoff.expo, max_time=10)
	def load_compound(self, link: bytes) -> Optional[Compound]:
		return next(self.col_t1.find({'_id': link}), None)


	@backoff.on_predicate(backoff.expo, max_time=10)
	def load_datapoints(self, dps_ids: List[DataPointId]) -> List[DataPoint]:
		"""
		Load datapoints in the order they appear in dps_ids.
		"""
		return (
			sorted(dps, key=lambda dp: dps_ids.index(dp["_id"]))
			if len(dps := list(self.col_t0.find({'_id': {"$in": dps_ids}}))) == len(dps_ids)
			else list()
		)


	@backoff.on_predicate(backoff.expo, max_time=10)
	def load_datapoint(self, link: DataPointId) -> Optional[DataPoint]:
		return next(self.col_t0.find({'_id': link}), None)


	@backoff.on_predicate(backoff.expo, max_time=10)
	def load_stock(self, link: StockId) -> Optional[StockDocument]:
		return next(self.col_stock.find({'_id': link}), None)


	def run_tied_queries(self,
		queries: List[Dict[str, Any]],
		t2_doc: T2Document,
		jupdater: JournalUpdater,
		logger: AmpelLogger
	) -> Union[T2SysRunState, List[T2DocView]]:

		t2_views: List[T2DocView] = []

		for query in queries:

			if self.run_dependent_t2s:

				processed_ids: List[ObjectId] = []

				# run pending dependencies
				while (
					dep_t2_doc := self.col_t2.find_one_and_update(
						{'status': self.status_match, **query},
						{'$set': {'status': T2SysRunState.RUNNING}}
					)
				) is not None:

					if not dep_t2_doc.get('body'):
						dep_t2_doc['body'] = []

					dep_t2_doc['body'].append(
						self.process_t2_doc(dep_t2_doc, logger, jupdater)
					)

					# suppress channel info
					dep_t2_doc.pop('channel')
					t2_views.append(self.view_from_record(dep_t2_doc))
					processed_ids.append(dep_t2_doc['_id'])

				if len(processed_ids) > 0:
					query['_id'] = {'$nin': processed_ids}

			# collect dependencies
			for dep_t2_doc in self.col_t2.find(query):
				# suppress channel info
				dep_t2_doc.pop('channel')
				t2_views.append(self.view_from_record(dep_t2_doc))

		if not self.run_dependent_t2s:
			for view in t2_views:
				if view.get_payload() is None:
					if logger.verbose > 1:
						logger.debug("Dependent T2 unit not run yet", extra={'t2_oid': t2_doc['_id']})
					return T2SysRunState.PENDING_DEPENDENCY

		return t2_views


	def view_from_record(self, doc: T2Document) -> T2DocView:
		"""
		We might want to move this method elsewhere in the future
		"""

		t2_unit_info = self.context.config.get(f'unit.base.{doc["unit"]}', dict)
		if not t2_unit_info:
			raise ValueError(f'Unknown T2 unit {doc["unit"]}')

		if 'AbsStockT2Unit' in t2_unit_info['base']:
			t2_type: int = TYPE_STOCK_T2
		elif 'AbsPointT2Unit' in t2_unit_info['base']:
			t2_type = TYPE_POINT_T2
		else: # quick n dirty
			t2_type = TYPE_STATE_T2

		return T2DocView(
			stock = doc['stock'],
			unit = doc['unit'],
			link = doc['link'],
			status = doc['status'],
			t2_type = t2_type,
			created = doc['_id'].generation_time.timestamp(),
			body = doc.get('body'),
			config = self.context.config.get(
				f'confid.{doc["config"]}', dict
			) if doc['config'] else None
		)


	def build_tied_t2_query(self,
		t2_unit: AbsT2, tied_model: UnitModel, t2_doc: T2Document
	) -> Dict[str, Any]:
		"""
		This method handles "default" situations.
		Callers must check returned 'link'.
		If None (state t2 linked with point t2), further handling is required

		:raises:
			- ValueError if functionality is not implemented yet
			- BadConfig if what's requested is not possible (a point T2 cannot be linked with a state t2)
	
		:returns: link value to match or None if further handling is required (state t2 linked with point t2)
		"""

		t2_unit_info = self.context.config.get(
			f'unit.base.{tied_model.unit}', dict
		)

		if not t2_unit_info:
			raise ValueError(f'Unknown T2 unit {tied_model.unit}')

		d: Dict[str, Any] = {
			'unit': tied_model.unit,
			'config': tied_model.config,
			'channel': {"$in": t2_doc['channel']},
			'stock': t2_doc['stock'],
		}

		if isinstance(t2_unit, AbsTiedPointT2Unit):

			if 'AbsPointT2Unit' in t2_unit_info['base']:
				d['link'] = t2_doc['link']

			elif 'AbsStockT2Unit' in t2_unit_info['base']:
				raise ValueError('Not implemented yet')

			else: # State T2
				raise BadConfig('Tied point T2 cannot be linked with state T2s')

		elif isinstance(t2_unit, AbsTiedStockT2Unit):

			if 'AbsPointT2Unit' in t2_unit_info['base']:
				raise BadConfig('Tied stock T2 cannot be linked with point T2s')

			elif 'AbsStockT2Unit' in t2_unit_info['base']:
				d['link'] = t2_doc['stock']

			else: # State T2
				raise BadConfig('Tied stock T2 cannot be linked with state T2s')

		if isinstance(t2_unit, (AbsTiedStateT2Unit, AbsTiedCustomStateT2Unit)):

			if 'AbsPointT2Unit' in t2_unit_info['base']:
				d['link'] = None # Further checks required (link_override check)

			elif 'AbsStockT2Unit' in t2_unit_info['base']:
				d['link'] = t2_doc['stock']

			else: # State T2
				d['link'] = t2_doc['link']

		return d


	def run_t2_unit(self,
		t2_unit: AbsT2, t2_doc: T2Document, logger: AmpelLogger, jupdater: JournalUpdater,
	) -> T2UnitResult:
		"""
		Regarding the possible int return code:
		usually, if an int is returned, it should be a T2RunState member
		but let's not be too restrictive here
		"""

		args = self.load_input_docs(t2_unit, t2_doc, logger, jupdater)

		if args is None:
			return T2RunState.ERROR

		if isinstance(args, int):
			return args

		ret = t2_unit.run(*args)

		if t2_unit._buf_hdlr.buffer: # type: ignore[union-attr]
			t2_unit._buf_hdlr.forward( # type: ignore[union-attr]
				logger, stock=t2_doc['stock'], channel=t2_doc['channel']
			)

		return ret


	def _processing_error(self,
		logger: AmpelLogger, t2_doc: T2Document, t2_rec: T2Record,
		j_rec: JournalRecord, rec_msg: str, report_msg: Optional[str] = None,
		extra: Dict[str, Any] = {}, exception: Optional[Exception] = None
	) -> None:
		"""
		- Updates the t2 document by appending a T2Record and
		updating 'status' with value T2RunState.EXCEPTION if an exception
		if provided through the parameter 'exception' or with value
		T2RunState.ERROR otherwise. The field  'msg' of the created
		T2Record entry will be set to the value of parameter 'rec_msg'.

		- Updates the stock document by appending a JournalRecord to it
		and by updating the 'modified' timestamp

		- Creates a 'trouble' document in the troubles collection
		to report the incident
		"""

		t2_rec = t2_rec.copy()
		t2_rec['msg'] = rec_msg

		self.push_t2_update(
			t2_doc['_id'], t2_rec, logger,
			status = T2SysRunState.EXCEPTION if exception else T2RunState.ERROR
		)

		info: Dict[str, Any] = {
			**extra,
			'process': self.process_name,
			'run': t2_rec['run'],
			'stock': t2_doc['stock'],
			'doc': t2_doc
		}

		if exception:
			self._exception = True
			report_exception(
				self._ampel_db, logger=logger,
				exc=exception, info=info
			)
		else:
			report_error(
				self._ampel_db, logger=logger,
				msg=report_msg if report_msg else '', info=info
			)


	def push_t2_update(self,
		t2_id: ObjectId, t2_rec: T2Record, logger: AmpelLogger, *,
		tag: Optional[Union[Tag, List[Tag]]] = None, status: int = T2RunState.COMPLETED
	) -> None:
		""" Performs DB updates of the T2 doc and stock journal """

		if logger.verbose:
			logger.log(VERBOSE, 'Saving T2 unit result')

		setd: Dict[str, Any] = {'status': status}
		if tag:
			setd['$addToSet'] = {'$each': tag} if isinstance(tag, list) else tag

		# Update T2 document
		self.col_t2.update_one(
			{'_id': t2_id},
			{
				'$push': {'body': t2_rec},
				'$set': setd
			}
		)


	def get_unit_instance(self, t2_doc: T2Document, logger: AmpelLogger) -> AbsT2:

		k = f'{t2_doc["unit"]}_{t2_doc["config"]}'

		# Check if T2 instance exists in this run
		if k not in self.t2_instances:

			if 'col' in t2_doc:

				if 't0' in t2_doc['col']:  # type: ignore[operator] # mypy inference issue
					sub_type: Type[AbsT2] = AbsPointT2Unit
				elif 'stock' in t2_doc['col']: # type: ignore[operator] # mypy inference issue
					sub_type = AbsStockT2Unit
				else:
					raise ValueError('Unsupported t2 unit type')

			else:

				bcs = self.context.config.get( # type: ignore[call-overload] # improve AmpelConfig overloads
					f"unit.base.{t2_doc['unit']}.base", (list, tuple)
				)

				if bcs is None:
					raise ValueError(f'Unknown t2 unit: {t2_doc["unit"]}')

				if "AbsStateT2Unit" in bcs:
					sub_type = AbsStateT2Unit
				elif "AbsCustomStateT2Unit" in bcs:
					sub_type = AbsCustomStateT2Unit
				elif "AbsTiedStateT2Unit" in bcs:
					sub_type = AbsTiedStateT2Unit
				elif "AbsTiedCustomStateT2Unit" in bcs:
					sub_type = AbsTiedCustomStateT2Unit
				else:
					raise ValueError(f'Unsupported t2 unit (base classes: {bcs})')

			# Create channel (buffering) logger
			buf_hdlr = DefaultRecordBufferingHandler(level=logger.level)
			buf_logger = AmpelLogger.get_logger(
				name = k,
				base_flag = (getattr(logger, 'base_flag', 0) & ~LogFlag.CORE) | LogFlag.UNIT,
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
			self.t2_instances[k] = unit_instance # type: ignore[assignment] # it's fine

		return self.t2_instances[k]
