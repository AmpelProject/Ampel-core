#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/abstract/AbsWorker.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 28.05.2021
# Last Modified Date: 05.09.2021
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import gc, signal
from time import time
from typing import ClassVar, Optional, Tuple, List, Union, Dict, Any, Sequence, TypeVar, Generic, Literal

from ampel.types import UBson, Tag
from ampel.base.decorator import abstractmethod
from ampel.base.LogicalUnit import LogicalUnit
from ampel.mongo.utils import maybe_match_array
from ampel.log.utils import convert_dollars
from ampel.enum.DocumentCode import DocumentCode
from ampel.enum.MetaActionCode import MetaActionCode
from ampel.content.T1Document import T1Document
from ampel.content.T2Document import T2Document
from ampel.log import AmpelLogger, LogFlag, VERBOSE, DEBUG
from ampel.core.EventHandler import EventHandler
from ampel.log.utils import report_exception, report_error
from ampel.log.handlers.DefaultRecordBufferingHandler import DefaultRecordBufferingHandler
from ampel.util.hash import build_unsafe_dict_id
from ampel.abstract.AbsEventUnit import AbsEventUnit
from ampel.model.UnitModel import UnitModel
from ampel.mongo.update.MongoStockUpdater import MongoStockUpdater
from ampel.metrics.AmpelMetricsRegistry import AmpelMetricsRegistry, Histogram, Counter

T = TypeVar("T", T1Document, T2Document)


class AbsWorker(Generic[T], AbsEventUnit, abstract=True):
	"""
	Fill results into pending :class:`T2 documents <ampel.content.T2Document.T2Document>`
	or :class:`T1 documents <ampel.content.T1Document.T1Document>`
	"""

	#: ids of the units to run. If not specified, any t1/t2 unit will be run.
	unit_ids: Optional[List[str]]

	#: process only with the given code
	code_match: Union[DocumentCode, Sequence[DocumentCode]] = [DocumentCode.NEW, DocumentCode.RERUN_REQUESTED]

	#: max number of docs to process in :func:`run`
	doc_limit: Optional[int]

	#: tag(s) to add to the stock :class:`~ampel.content.JournalRecord.JournalRecord`
	#: every time a document is processed
	stock_jtag: Optional[Union[Tag, Sequence[Tag]]]

	#: create a 'beacon' document indicating the last time T2Processor was run
	#: in the current configuration
	send_beacon: bool = True

	#: explicitly call :func:`gc.collect` after every document
	garbage_collect: bool = True

	#: maximum number of processing attempts per document
	max_try: int = 5

	tier: ClassVar[Literal[1, 2]]

	#: For later
	database: str = "mongo"


	def __init__(self, **kwargs) -> None:

		super().__init__(**kwargs)
		self._ampel_db = self.context.db
		self._loader = self.context.loader

		# Prepare DB query dict
		self.query: Dict[str, Any] = {'code': maybe_match_array(self.code_match)} # type: ignore[arg-type]

		# Possibly restrict query to specified t2 units
		if self.unit_ids:
			self.query['unit'] = maybe_match_array(self.unit_ids)

		# Shortcut
		self.col_stock = self._ampel_db.get_collection('stock')
		self.col_t0 = self._ampel_db.get_collection('t0')
		self.col_t1 = self._ampel_db.get_collection('t1')
		self.col = self._ampel_db.get_collection(f't{self.tier}')

		if self.send_beacon:
			self.create_beacon()

		self._run = True
		self._doc_counter = 0
		signal.signal(signal.SIGTERM, self.sig_exit)
		signal.signal(signal.SIGINT, self.sig_exit)

		# _instances stores unit instances so that they can be re-used in run()
		# Key: set(unit name + config), value: unit instance
		self._instances: Dict[str, Any] = {}


	@abstractmethod
	def process_doc(self,
		doc: T, stock_updr: MongoStockUpdater, logger: AmpelLogger
	) -> Any:
		...


	def run(self, pre_check: bool = True) -> int:
		""" :returns: number of t2 docs processed """

		# Add new doc in the 'events' collection
		event_hdlr = EventHandler(
			self._ampel_db, process_name=self.process_name, tier=2
		)

		# Avoid 'burning' a run_id for nothing (at the cost of a request)
		if pre_check and self.col.find(self.query).count() == 0:
			return 0

		run_id = self.context.new_run_id()

		logger = AmpelLogger.from_profile(
			self.context, self.log_profile, run_id,
			base_flag = getattr(LogFlag, f'T{self.tier}') | LogFlag.CORE | self.base_log_flag
		)

		if self.send_beacon:
			self.col_beacon.update_one(
				{'_id': self.beacon_id},
				{'$set': {'timestamp': int(time())}}
			)

		stock_updr = MongoStockUpdater(
			ampel_db = self.context.db, tier = self.tier, run_id = run_id,
			process_name = self.process_name, logger = logger,
			raise_exc = self.raise_exc, extra_tag = self.stock_jtag
		)

		# Loop variables
		self._doc_counter = 0
		update = {'$set': {'code': DocumentCode.RUNNING}}
		garbage_collect = self.garbage_collect
		doc_limit = self.doc_limit

		# Process docs until next() returns None (breaks condition below)
		self._run = True
		while self._run:

			# get t1/t2 document (code is usually NEW or NEW_PRIO)
			doc = self.col.find_one_and_update(self.query, update)

			# Cursor exhausted
			if doc is None:
				break
			elif logger.verbose > 1:
				logger.debug(f'T{self.tier} doc to process: {doc}')

			self.process_doc(doc, stock_updr, logger)

			# Check possibly defined doc_limit
			if doc_limit and self._doc_counter >= doc_limit:
				break

			if garbage_collect:
				gc.collect()

		event_hdlr.update(logger, docs=self._doc_counter, run=run_id)

		logger.flush()
		self._instances.clear()
		return self._doc_counter


	def _processing_error(self,
		logger: AmpelLogger, doc: T, body: UBson,
		meta: Dict[str, Any], msg: Optional[str] = None,
		extra: Optional[Dict[str, Any]] = None, exception: Optional[Exception] = None
	) -> None:
		"""
		- Updates the t1/t2 document by appending a meta entry and
		updating 'code' with value DocumentCode.EXCEPTION if an exception
		was provided through the parameter 'exception' or with value
		DocumentCode.ERROR otherwise.

		- Updates the stock document by appending a JournalRecord to it
		and by updating the 'updated' timestamp

		- Creates a 'trouble' document in the troubles collection to report the incident

		:param msg: added to meta entry if provided
		"""

		self.commit_update(
			{'_id': doc['_id']}, # type: ignore
			meta | {
				'code': DocumentCode.EXCEPTION if exception else DocumentCode.ERROR,
				'msg': msg
			},
			logger
		)

		info: Dict[str, Any] = (extra or {}) | meta | {'stock': doc['stock'], 'doc': doc}

		if exception:
			report_exception(self._ampel_db, logger=logger, exc=exception, info=info)
		else:
			report_error(self._ampel_db, logger=logger, msg=msg, info=info)


	def commit_update(self,
		match: Dict[str, Any], meta: Dict[str, Any], logger: AmpelLogger, *,
		payload_op: Literal['$push', '$set'] = '$push',
		body: UBson = None, code: int = 0
	) -> None:
		""" Insert/upsert tier docs into DB """

		if logger.verbose:
			logger.log(VERBOSE, f'Saving T{self.tier} unit result')
			if logger.verbose > 1:
				if body is None:
					logger.log(DEBUG, "No body returned")
				else:
					logger.log(DEBUG, None, extra={"body": body})

		upd: Dict[str, Any] = {
			'$set': {'code': code},
			'$push': {'meta': meta}
		}

		if body is not None:
			upd[payload_op]['body'] = body

		# Update document
		self.col.update_one(match, upd)


	def gen_meta(self, run_id: int, unit_trace_id: int, duration: Union[int, float]) -> Dict[str, Any]:
		return {
			'run': run_id,
			'ts': int(time()),
			'tier': self.tier,
			'code': None,
			'action': MetaActionCode(0),
			'duration': duration,
			'traceid': {
				f't{self.tier}worker': self._trace_id,
				f't{self.tier}unit': unit_trace_id
			}
		}


	def sig_exit(self, signum: int, frame) -> None:
		""" Executed when SIGTERM/SIGINT is caught. Stops doc processing in run() """
		self._run = False


	def create_beacon(self) -> None:
		""" Creates a beacon document if it does not exist yet """

		args = {
			'class': self.__class__.__name__,
			'unit': self.unit_ids,
			'code': self.code_match,
			'base_log_flag': self.base_log_flag.__int__(),
			'doc_limit': self.doc_limit
		}

		self.beacon_id = build_unsafe_dict_id(args)

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


	def get_unit_instance(self,
		doc: Union[T1Document, T2Document],
		logger: AmpelLogger
	) -> LogicalUnit:

		k = f'{doc["unit"]}_{doc["config"]}'

		# Check if T2 instance exists in this run
		if k not in self._instances:

			# Create channel (buffering) logger
			buf_hdlr = DefaultRecordBufferingHandler(level=logger.level)
			buf_logger = AmpelLogger.get_logger(
				name = k,
				base_flag = (getattr(logger, 'base_flag', 0) & ~LogFlag.CORE) | LogFlag.UNIT,
				console = False,
				handlers = [buf_hdlr]
			)

			# Instantiate unit
			unit_instance = self._loader.new_logical_unit(
				model = UnitModel(unit = doc['unit'], config = doc['config']),
				logger = buf_logger
			)

			# Shortcut to avoid unit_instance.logger.handlers[?]
			setattr(unit_instance, '_buf_hdlr', buf_hdlr)
			self._instances[k] = unit_instance

		return self._instances[k]


def register_stats(tier: int) -> Tuple[Histogram, Counter]:

	hist = AmpelMetricsRegistry.histogram(
		'latency',
		f'Delay between T{tier} doc creation and processing',
		subsystem=f't{tier}',
		unit='seconds',
		labelnames=('unit', ),
	)

	counter = AmpelMetricsRegistry.counter(
		'docs_processed',
		f'Number of T{tier} documents processed',
		subsystem=f't{tier}',
		labelnames=('unit', )
	)

	return hist, counter
