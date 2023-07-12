#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/abstract/AbsWorker.py
# License:             BSD-3-Clause
# Author:              valery brinnel <firstname.lastname@gmail.com>
# Date:                28.05.2021
# Last Modified Date:  15.12.2022
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

import gc, signal, sys
import random
from math import ceil
from time import time, sleep
from typing import ClassVar, Any, TypeVar, Generic, Literal, Sequence, Generator
from ampel.base.AmpelBaseModel import AmpelBaseModel
from contextlib import contextmanager
from threading import Event

from pymongo.write_concern import WriteConcern
from pymongo.read_concern import ReadConcern

from ampel.types import OneOrMany, JDict, UBson, Tag
from ampel.base.decorator import abstractmethod
from ampel.base.LogicalUnit import LogicalUnit
from ampel.mongo.utils import maybe_match_array
from ampel.log.utils import convert_dollars
from ampel.enum.DocumentCode import DocumentCode
from ampel.content.MetaRecord import MetaRecord
from ampel.content.T1Document import T1Document
from ampel.content.T2Document import T2Document
from ampel.enum.MetaActionCode import MetaActionCode
from ampel.enum.EventCode import EventCode
from ampel.log import AmpelLogger, LogFlag, VERBOSE, DEBUG
from ampel.core.EventHandler import EventHandler
from ampel.log.utils import report_exception, report_error
from ampel.log.handlers.DefaultRecordBufferingHandler import DefaultRecordBufferingHandler
from ampel.util.hash import build_unsafe_dict_id
from ampel.abstract.AbsEventUnit import AbsEventUnit
from ampel.abstract.AbsUnitResultAdapter import AbsUnitResultAdapter
from ampel.model.UnitModel import UnitModel
from ampel.mongo.update.MongoStockUpdater import MongoStockUpdater
from ampel.mongo.utils import maybe_use_each
from ampel.metrics.AmpelMetricsRegistry import AmpelMetricsRegistry, Histogram, TimingCounter, Summary
from ampel.util.tag import merge_tags

T = TypeVar("T", T1Document, T2Document)

class BackoffConfig(AmpelBaseModel):
	base: float = 2
	factor: float = 1
	jitter: bool = True
	max_value: float = 10.
	max_tries: None | int = None
	max_time: None | float = 60.

stat_time = AmpelMetricsRegistry.summary(
    "time",
    "Processing time",
    unit="microseconds",
    subsystem="worker",
    labelnames=("tier", "phase", "unit"),
)

@contextmanager
def stop_on_signal(signals: Sequence[signal.Signals], logger: AmpelLogger) -> Generator[Event, None, None]:
	stop_token = Event()
	def handle_signal(signum: int, frame):
		logger.log(LogFlag.SHOUT, f"caught {signal.Signals(signum).name}")
		stop_token.set()
	prev_handlers = {sig: signal.signal(sig, handle_signal) for sig in signals}
	try:
		yield stop_token
	finally:
		for sig, handler in prev_handlers.items():
			signal.signal(sig, handler)

class AbsWorker(Generic[T], AbsEventUnit, abstract=True):
	"""
	Fill results into pending :class:`T2 documents <ampel.content.T2Document.T2Document>`
	or :class:`T1 documents <ampel.content.T1Document.T1Document>`
	"""

	#: Ids of the units to run. If not specified, any t1/t2 unit will be run.
	unit_ids: None | list[str]

	#: Process only with the given code
	code_match: OneOrMany[int] = [DocumentCode.NEW, DocumentCode.RERUN_REQUESTED]

	#: Max number of docs to process in :func:`run`
	doc_limit: None | int

	#: Tag(s) to add to the stock :class:`~ampel.content.JournalRecord.JournalRecord`
	#: every time a document is processed
	jtag: None | OneOrMany[Tag]

	#: Tag(s) to add to the stock :class:`~ampel.content.MetaRecord.MetaRecord`
	#: every time a document is processed
	mtag: None | OneOrMany[Tag]

	#: Create a 'beacon' document indicating the last time T2Processor was run
	#: in the current configuration
	send_beacon: bool = True

	#: Explicitly call :func:`gc.collect` after the processing of each t1/t2 document.
	#: Turn this on if units do not clean up resources adequately,
	#: but beware of the performance impact.
	garbage_collect: bool = False

	#: Maximum number of processing attempts per document
	max_try: int = 5

	tier: ClassVar[Literal[1, 2]]

	#: Minimum age of documents to be matched (in seconds)
	#: {'$expr': {'$lt': [{'$last': '$meta.ts'}, now - min_doc_age]}}
	min_doc_age: None | float

	#: One day, we might support different DBs
	# database: str = "mongo"

	#: Avoid 'burning' a run_id for nothing at the cost of a request
	pre_check: bool = True

	#: Wait for documents to process
	backoff_on_query: None | BackoffConfig = None

	#: wait for majority acknowledgement of document updates. this introduces
	#: significant extra latency for replicated mongo clusters.
	wait_for_durable_write: bool = True

	#: read only commmitted updates
	durable_read: bool = False

	#: minimum number of stock document updates to commit at once
	updates_buffer_size: int = 500

	def __init__(self, **kwargs) -> None:

		super().__init__(**kwargs)
		self._ampel_db = self.context.db
		self._loader = self.context.loader

		# Prepare DB query dict
		self.query: JDict = {
			'code': self.code_match if isinstance(self.code_match, DocumentCode)
				else maybe_match_array(self.code_match) # type: ignore[arg-type]
		}

		# Possibly restrict query to specified t2 units
		if self.unit_ids:
			self.query['unit'] = maybe_match_array(self.unit_ids)

		if self.min_doc_age:
			self.query['$expr'] = {
				'$lt': [{'$last': '$meta.ts'}, time() - self.min_doc_age]
			}

		# Shortcut
		self.col_stock = self._ampel_db.get_collection('stock', mode='r')
		self.col_t0 = self._ampel_db.get_collection('t0', mode='r')
		self.col_t1 = self._ampel_db.get_collection('t1', mode='r')
		self.col = self._ampel_db.get_collection(f't{self.tier}', mode='w')
		if not self.wait_for_durable_write:
			# Only wait for the primary to acknowledge (in-memory) write, rather
			# than a majority of replica set members to acknowledge write to
			# their journals. This has much lower latency, but can result in doc
			# updates being lost if the primary goes down before the write can
			# be replicated, but if this happens the doc can simply be rerun.
			self.col = self.col.with_options(write_concern=WriteConcern(w=1, j=False))
		if self.durable_read:
			self.col = self.col.with_options(read_concern=ReadConcern(level="majority"))

		if self.send_beacon:
			self.create_beacon()

		# _instances stores unit instances so that they can be re-used in run()
		# Key: set(unit name + config), value: unit instance
		self._instances: JDict = {}

		self._current_run_id: None | int = None
		self._adapters: dict[int, AbsUnitResultAdapter] = {}


	@abstractmethod
	def process_doc(self,
		doc: T, stock_updr: MongoStockUpdater, logger: AmpelLogger
	) -> Any:
		...


	def prepare(self, event_hdlr: EventHandler) -> None | EventCode:
		""" :returns: number of t2 docs processed """
		event_hdlr.set_tier(2)
		if self.pre_check and self.col.count_documents(self.query) == 0:
			return EventCode.PRE_CHECK_EXIT
		return None


	def find_one_and_update(self, query: dict, update: dict, stop_token: Event) -> None | T:
		"""
		Get next eligible document, with timeout
		"""
		t0 = time()
		attempts = 0
		while not stop_token.is_set():
			doc = self.col.find_one_and_update(
				query | {'$expr': {'$not': {'$lte': [ceil(time()), {'$last': '$meta.retry_after'}]}}},
				update
			)
			if doc is not None or self.backoff_on_query is None:
				return doc
			backoff = self.backoff_on_query
			if backoff.max_time is not None and time()-t0 > backoff.max_time:
				break
			if backoff.max_tries is not None and attempts+1 >= backoff.max_tries:
				break
			delay = min(backoff.factor*(backoff.base**attempts), backoff.max_value)
			if backoff.jitter:
				delay = random.uniform(0, delay)
			stop_token.wait(delay)
			attempts += 1
		
		return None


	def proceed(self, event_hdlr: EventHandler) -> int:
		""" :returns: number of t2 docs processed """

		event_hdlr.set_tier(self.tier)
		run_id = event_hdlr.get_run_id()

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
			raise_exc = self.raise_exc, extra_tag = self.jtag
		)

		# Loop variables
		doc_counter = 0
		update = {'$set': {'code': DocumentCode.RUNNING}}
		garbage_collect = self.garbage_collect
		doc_limit = self.doc_limit

		with stop_on_signal(
			[signal.SIGINT, signal.SIGTERM, signal.SIGQUIT, signal.SIGHUP],
			logger
		) as stop_token:
			try:
				self._current_run_id = run_id
				# Process docs until next() returns None (breaks condition below)
				while not stop_token.is_set():

					# get t1/t2 document (code is usually NEW or NEW_PRIO), excluding
					# docs with retry times in the future
					with stat_time.time() as timer:
						doc = self.find_one_and_update(self.query, update, stop_token)
						timer.labels(self.tier, "find_one_and_update", doc["unit"] if doc else None)

					# No match
					if doc is None:
						if not stop_token.is_set():
							logger.log(LogFlag.SHOUT, "No more docs to process")
						break
					elif logger.verbose > 1:
						logger.debug(f'T{self.tier} doc to process', extra={'doc': doc})

					with stat_time.labels(self.tier, "process_doc", doc["unit"]).time():
						self.process_doc(doc, stock_updr, logger)
					doc_counter += 1

					# Check possibly defined doc_limit
					if doc_limit and doc_counter >= doc_limit:
						break

					if garbage_collect:
						gc.collect()
			finally:
				stock_updr.flush()
				event_hdlr.add_extra(docs=doc_counter)

				logger.flush()
				self._instances.clear()
				self._adapters.clear()
				self._current_run_id = None

		return doc_counter


	def _processing_error(self,
		logger: AmpelLogger, doc: T, body: UBson,
		meta: MetaRecord, msg: None | str = None,
		extra: None | JDict = None, exception: None | Exception = None
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

		if (
			not meta['activity'] or
			(len(meta['activity']) == 1 and meta['activity'][0]['action'] == 0)
		):
			meta['activity'] = []

		if not [el for el in meta['activity'] if (el['action'] & MetaActionCode.SET_CODE)]:
			meta['activity'].append( # type: ignore[attr-defined]
				{'action': MetaActionCode.SET_CODE}
			)

		if 'extra' not in meta:
			meta['extra'] = {}

		meta['extra']['msg'] = msg

		self.commit_update({'_id': doc['_id']}, meta, logger, code=DocumentCode.EXCEPTION) # type: ignore[typeddict-item]

		info: JDict = (extra or {}) | meta | {'stock': doc['stock'], 'doc': doc}
		if exception:
			report_exception(self._ampel_db, logger=logger, exc=exception, info=info)
		else:
			report_error(self._ampel_db, logger=logger, msg=msg, info=info)


	def commit_update(self,
		match: JDict,
		meta: MetaRecord,
		logger: AmpelLogger, *,
		payload_op: Literal['$push', '$set'] = '$push',
		body: UBson = None,
		tag: None | OneOrMany[Tag] = None,
		code: int = 0
	) -> None:
		"""
		Insert/upsert tier docs into DB.
		"""

		if logger.verbose:
			if logger.verbose > 1:
				if body is None:
					logger.log(DEBUG, "T2 unit returned no body")
				else:
					logger.log(DEBUG, f'Saving T{self.tier} unit result', extra={"body": body})
			else:
				logger.log(VERBOSE, f'Saving T{self.tier} unit result')

		upd: JDict = {
			'$set': {'code': code},
			'$push': {'meta': meta}
		}

		if self.mtag:

			tag = merge_tags(self.mtag, tag) if tag else self.mtag # type: ignore
			activities = meta['activity']

			# T2 unit added a tag, make the distinction clear by adding a dedicated activity
			if [el for el in activities if 'tag' in el]:
				activities.append( # type: ignore[attr-defined]
					{'action': MetaActionCode.ADD_WORKER_TAG, 'tag': self.mtag}
				)
			else:
				activities[0]['action'] |= MetaActionCode.ADD_WORKER_TAG
				activities[0]['tag'] = self.mtag

		if tag:
			upd['$addToSet'] = {
				'tag': tag if isinstance(tag, (int, str)) else maybe_use_each(tag)
			}

		if body is not None:
			upd[payload_op]['body'] = body

		# Update document
		self.col.update_one(match, upd)


	def gen_meta(self,
		run_id: int,
		unit_trace_id: None | int,
		duration: int | float,
		action_code: MetaActionCode = MetaActionCode(0)
	) -> MetaRecord:

		d: MetaRecord = {
			'run': run_id,
			'ts': int(time()),
			'tier': self.tier,
			'code': None, # type: ignore[typeddict-item]
			'duration': duration,
			'activity': [{'action': action_code}],
			'traceid': {
				f't{self.tier}worker': self._trace_id,
				f't{self.tier}unit': unit_trace_id
			}
		}

		if self.job_sig:
			d['jobid'] = self.job_sig

		return d

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
		doc: T1Document | T2Document,
		logger: AmpelLogger
	) -> LogicalUnit:

		k = f'{doc["unit"]}_{doc["config"]}'

		# Check if T2 instance exists in this run
		if k not in self._instances:

			# Create channel (buffering) logger
			buf_hdlr = DefaultRecordBufferingHandler(level=logger.level)
			buf_hdlr._unit = doc['unit']
			buf_logger = AmpelLogger.get_logger(
				name = k,
				base_flag = (getattr(logger, 'base_flag', 0) & ~LogFlag.CORE) | LogFlag.UNIT,
				console = False,
				handlers = [buf_hdlr]
			)

			# Instantiate unit
			unit_instance = self._loader.new_logical_unit(
				model = UnitModel(unit=doc['unit'], config=doc['config']),
				logger = buf_logger
			)

			# Shortcut to avoid unit_instance.logger.handlers[?]
			setattr(unit_instance, '_buf_hdlr', buf_hdlr)
			self._instances[k] = unit_instance

		return self._instances[k]


	def get_adapter_instance(self, model: UnitModel) -> AbsUnitResultAdapter:
		assert self._current_run_id is not None
		config_id = build_unsafe_dict_id(model.dict())

		if config_id not in self._adapters:
			unit_instance = self._loader.new_context_unit(
				model = model,
				context = self.context,
				run_id = self._current_run_id,
				sub_type = AbsUnitResultAdapter,
			)
			self._adapters[config_id] = unit_instance

		return self._adapters[config_id]


def register_stats(tier: int) -> tuple[Histogram, TimingCounter, Summary]:

	latency = AmpelMetricsRegistry.histogram(
		'latency',
		f'Delay between T{tier} doc creation and processing',
		subsystem=f't{tier}',
		unit='seconds',
		labelnames=('unit', ),
	)

	docs = AmpelMetricsRegistry.counter(
		'docs_processed',
		f'Number of T{tier} documents processed',
		subsystem=f't{tier}',
		labelnames=('unit', 'code',)
	)

	processing_time = AmpelMetricsRegistry.summary(
		'processing_time',
		f'Time spent processing of T{tier} documents processed',
		subsystem=f't{tier}',
		unit='microseconds',
		labelnames=('unit', 'phase')
	)

	return latency, docs, processing_time
