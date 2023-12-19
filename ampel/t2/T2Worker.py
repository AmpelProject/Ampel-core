#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/t2/T2Worker.py
# License:             BSD-3-Clause
# Author:              valery brinnel <firstname.lastname@gmail.com>
# Date:                24.05.2019
# Last Modified Date:  28.08.2022
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

import random
from math import ceil
from time import time
from bson import ObjectId
from importlib import import_module
from typing import Union, Any, ClassVar, Literal
from collections.abc import Sequence

from ampel.types import T, OneOrMany, UBson, ubson
from ampel.struct.UnitResult import UnitResult
from ampel.enum.DocumentCode import DocumentCode
from ampel.enum.MetaActionCode import MetaActionCode
from ampel.enum.JournalActionCode import JournalActionCode
from ampel.content.StockDocument import StockDocument
from ampel.content.DataPoint import DataPoint
from ampel.content.T1Document import T1Document
from ampel.content.T2Document import T2Document
from ampel.base.BadConfig import BadConfig
from ampel.log import AmpelLogger
from ampel.log.utils import convert_dollars
from ampel.log.utils import report_exception, report_error
from ampel.abstract.AbsStockT2Unit import AbsStockT2Unit
from ampel.abstract.AbsPointT2Unit import AbsPointT2Unit
from ampel.abstract.AbsStateT2Unit import AbsStateT2Unit
from ampel.abstract.AbsCustomStateT2Unit import AbsCustomStateT2Unit
from ampel.abstract.AbsTiedT2Unit import AbsTiedT2Unit
from ampel.abstract.AbsTiedPointT2Unit import AbsTiedPointT2Unit
from ampel.abstract.AbsTiedStateT2Unit import AbsTiedStateT2Unit
from ampel.abstract.AbsTiedStockT2Unit import AbsTiedStockT2Unit
from ampel.abstract.AbsTiedCustomStateT2Unit import AbsTiedCustomStateT2Unit, U
from ampel.abstract.AbsWorker import AbsWorker, register_stats
from ampel.base.AmpelBaseModel import AmpelBaseModel
from ampel.model.UnitModel import UnitModel
from ampel.model.StateT2Dependency import StateT2Dependency
from ampel.mongo.utils import maybe_match_array
from ampel.mongo.update.MongoStockUpdater import MongoStockUpdater
from ampel.view.T2DocView import T2DocView

AbsT2 = Union[
	AbsStockT2Unit, AbsPointT2Unit, AbsStateT2Unit, AbsTiedPointT2Unit,
	AbsTiedStateT2Unit, AbsCustomStateT2Unit[T], AbsTiedCustomStateT2Unit[T, U]
]

abs_t2 = (
	AbsStockT2Unit, AbsPointT2Unit, AbsStateT2Unit, AbsTiedPointT2Unit,
	AbsTiedStateT2Unit, AbsCustomStateT2Unit, AbsTiedCustomStateT2Unit
)

stat_latency, stat_count, stat_time = register_stats(tier=2)

class BackoffConfig(AmpelBaseModel):
	code_match: Sequence[int] = [
		DocumentCode.T2_PENDING_DEPENDENCY, # input t2 doc has not yet resolved
		DocumentCode.T2_MISSING_DEPENDENCY, # input t2 docs not found
		DocumentCode.T2_UNKNOWN_LINK, # input t1 doc not found
		DocumentCode.T2_MISSING_INFO, # t0 docs not found
	]
	base: float = 2
	factor: float = 1
	jitter: bool = True

DOCUMENT_CODES: dict[int, str] = {v: k for k,v in DocumentCode.__members__.items()}

class T2Worker(AbsWorker[T2Document]):
	"""
	Fill results into pending :class:`T2 documents <ampel.content.T2Document.T2Document>`.
	"""

	#: process only those :class:`T2 documents <ampel.content.T2Document.T2Document>`
	#: with the given :attr:`~ampel.content.T2Document.T2Document.code`
	code_match: OneOrMany[int] = [
		DocumentCode.NEW,
		DocumentCode.RERUN_REQUESTED,
		DocumentCode.T2_NEW_PRIO,
		DocumentCode.T2_PENDING_DEPENDENCY
	]

	#: Process dependencies of each T2 doc if they are not already marked OK.
	#: You likely want to disable this when running multiple workers against a
	#: replicated database in order to reduce the chance that concurrent
	#: updates interfere with the retry mechanism.
	run_dependent_t2s: bool = True
	tier: ClassVar[Literal[2]] = 2

	#: Add an exponentially increasing delay on codes that indicate temporary
	#: failures. Different backoff strategies may be specified for different codes
	backoff_on_retry: None | list[BackoffConfig] = [BackoffConfig()]

	#: dependency codes to consider transient
	pending_codes: list[int] = [
		DocumentCode.T2_PENDING_DEPENDENCY, # input t2 doc has not yet resolved
		DocumentCode.T2_MISSING_DEPENDENCY, # input t2 docs not found
		DocumentCode.T2_UNKNOWN_LINK, # input t1 doc not found
		DocumentCode.T2_MISSING_INFO, # t0 docs not found
		DocumentCode.RUNNING, # input t2 doc handled in another process
		DocumentCode.NEW, # input t2 doc not yet handled
		DocumentCode.T2_NEW_PRIO,
		DocumentCode.TOO_MANY_TRIALS,
	]

	def __init__(self, **kwargs):
		super().__init__(**kwargs)
		self._backoff_on_retry = {
			code: backoff
			for backoff in (self.backoff_on_retry or [])
			for code in backoff.code_match
		}
		self._pending_codes = set(self.pending_codes)


	def _get_trials(self, doc: T2Document):
		"""Number of attempts to resolve this document"""
		return len([el for el in doc['meta'] if el['tier'] == 2])


	def _is_retriable(self, doc: T2Document, code: DocumentCode):
		"""Would we retry this document later?"""
		return code in self._pending_codes and self._get_trials(doc) < self.max_try


	def process_doc(self,
		doc: T2Document,
		stock_updr: MongoStockUpdater,
		logger: AmpelLogger
	) -> tuple[UBson, int]:

		before_run = time()

		try:
			t2_unit = self.get_unit_instance(doc, logger)

			if not isinstance(t2_unit, abs_t2):
				raise ValueError(f"Unsupported unit: {doc['unit']}")

		except Exception as e:

			if self.raise_exc:
				raise

			self._processing_error(
				logger, doc, None, exception=e, msg='Could not instantiate unit',
				meta = self.gen_meta(stock_updr.run_id, None, 0)
			)

			return None, DocumentCode.EXCEPTION

		if (trials := self._get_trials(doc)) <= self.max_try:
			with stat_time.labels(doc["unit"], "run").time():
				ret = self.run_t2_unit(t2_unit, doc, logger, stock_updr)
		else:
			ret = UnitResult(code=DocumentCode.TOO_MANY_TRIALS)

		# Used as timestamp and to compute duration below (using before_run)
		now = time()

		body = None
		tag = None
		code = 0

		try:

			# New (channel-less) journal entry for the associated stock document
			trace_id = (
				({'t2worker': self._trace_id} if self._trace_id else {}) |
				({'t2unit': t2_unit._trace_id} if t2_unit._trace_id else {})
			)

			jrec = stock_updr.add_journal_record(
				stock = doc['stock'],
				channel = doc['channel'],
				doc_id = doc['_id'], # type: ignore
				trace_id = trace_id or None,
				unit = doc['unit']
			)

			# New meta entry (code appended later)
			meta = self.gen_meta(
				stock_updr.run_id,
				t2_unit._trace_id,
				round(now - before_run, 3),
				MetaActionCode.BUMP_STOCK_UPD
			)

			# The channel-less activity of this t2 worker
			activity = meta['activity'][0]

			# Unit requested customizations
			if isinstance(ret, UnitResult):

				if ret.adapter_model:
					ret = self.get_adapter_instance(ret.adapter_model).handle(ret)

				if ret.body:
					body = ret.body
					activity['action'] |= MetaActionCode.ADD_BODY
					jrec['action'] |= JournalActionCode.T2_ADD_BODY

				if ret.tag:
					tag = ret.tag
					activity['action'] |= MetaActionCode.ADD_UNIT_TAG
					activity['tag'] = ret.tag
					jrec['action'] |= JournalActionCode.T2_ADD_TAG

				if ret.code:
					code = ret.code
					activity['action'] |= MetaActionCode.SET_UNIT_CODE
					jrec['action'] |= JournalActionCode.T2_SET_CODE
					# set retry time for missing dependencies, similar to
					# https://github.com/litl/backoff
					if (backoff := self._backoff_on_retry.get(ret.code, None)):
						delay = backoff.factor * (backoff.base ** trials)
						if backoff.jitter:
							delay = random.uniform(0, delay)
						meta['retry_after'] = meta['ts'] + ceil(delay)

				# TODO: check that unit did not use system reserved code
				if code != 0 and code in DocumentCode.__members__.values():
					logger.info(
						f'T2 unit {t2_unit.__class__.__name__} returned document '
						f'code: {code} ({DocumentCode(code).name})',
						extra={'unit': doc['unit'], 'stock': doc['stock']}
					)

				if ret.journal:
					ret.journal.into(jrec)
					activity['action'] |= MetaActionCode.EXTRA_JOURNAL
					jrec['action'] |= JournalActionCode.T2_EXTRA_JOURNAL

			# Unit returned bson-like content
			elif isinstance(ret, ubson):
				body = ret
				activity['action'] |= MetaActionCode.ADD_BODY
				jrec['action'] |= JournalActionCode.T2_ADD_BODY

			# Unsupported object returned by unit
			else:
				code = DocumentCode.ERROR
				self._processing_error(
					logger, doc, None, meta, extra={'ret': ret},
					msg='Invalid content returned by T2 unit'
				)

			meta['code'] = code
			with stat_time.labels(doc["unit"], "commit_update").time():
				self.commit_update(
					{'_id': doc['_id']}, # type: ignore[typeddict-item]
					meta, logger, body=body, tag=tag, code=code
				)

			# Update stock document
			if len(stock_updr._updates) >= self.updates_buffer_size:
				stock_updr.flush()

		except Exception as e:

			if self.raise_exc:
				raise e

			self._processing_error(
				logger, doc, None, exception=e, msg='An exception occured',
				meta = self.gen_meta(stock_updr.run_id, t2_unit._trace_id, round(now - before_run, 3))
			)
			code = DocumentCode.EXCEPTION

		# _id is an ObjectId, but declared as bytes in ampel-interface to avoid
		# an explicit dependency on pymongo
		if doc['meta']: # robustify against manual changes of the db
			stat_latency.labels(doc['unit']).observe(now - doc['meta'][0]['ts'])
		stat_count.labels(doc['unit'], DOCUMENT_CODES.get(code, 'unknown')).inc()

		return body, code


	# NB: spell out union arg to ensure a common context for the TypeVar T
	def load_input_docs(self,
		t2_unit: AbsT2, t2_doc: T2Document, logger: AmpelLogger, stock_updr: MongoStockUpdater
	) -> Union[
		None,
		UnitResult,                                              # Error / missing dependency
		DataPoint,                                               # point t2
		tuple[DataPoint, T2DocView],                             # tied point t2
		tuple[StockDocument],                                    # stock t2
		tuple[T1Document, Sequence[DataPoint]],                  # state t2
		tuple[T],                                                # custom state t2 (T could be LightCurve)
		tuple[T1Document, Sequence[DataPoint], list[T2DocView]], # tied state t2
		tuple[T, list[T2DocView]]                                # tied custom state t2
	]:
		"""
		Fetches documents required by `t2_unit`.

		:param t2_unit: instance to fetch input docs for
		:param t2_doc: document for which inputs are needed
		:param logger: logger
		:param stock_updr: journal update service

		Regarding tied T2s (unit depends on other T2s):
		
		- Dependent T2s will be executed if run_dependent_t2s is True.
		- Note that the ingester ingests point and state t2s before state t2s so that the natural ordering
		  of T2 docs to be processed fits rather well a setup with a single T2processor instance processing all T2s.
		- For state t2s tied with other state t2s, putting the dependent units first in the AlertConsumer
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
			dps: list[DataPoint] = []
			with stat_time.labels(t2_doc["unit"], "load_t1").time():
				t1_doc: None | T1Document = next(
					self.col_t1.find(
						{'stock': t2_doc['stock'], 'link': t2_doc['link']}
					), None
				)

			# compound doc must exist (None could mean an ingester bug)
			if t1_doc is None:
				if not self._is_retriable(t2_doc, DocumentCode.T2_UNKNOWN_LINK):
					report_error(
						self._ampel_db, msg='T1Document not found', logger=logger,
						info={'id': t2_doc['link'], 'doc': t2_doc}
					)
				return UnitResult(code=DocumentCode.T2_UNKNOWN_LINK)

			# Datarights: suppress channel info (T3 uses instead a
			# 'projection' procedure that should not be necessary here)
			t1_doc.pop('channel') # type: ignore[misc]

			t1_dps_ids = list(t1_doc['dps'])

			# Sort DPS from DB in the same order than referenced by 'dps' from t1 doc
			with stat_time.labels(t2_doc["unit"], "load_t0").time():
				dps = sorted(
					self.col_t0.find({'id': {'$in': t1_dps_ids}}),
					key = lambda dp: t1_dps_ids.index(dp['id'])
				)

			# Should never happen (only in case of ingestion bug)
			if not dps:
				if not self._is_retriable(t2_doc, DocumentCode.T2_MISSING_INFO):
					report_error(
						self._ampel_db, msg='Datapoints not found',
						logger=logger, info={'t1': t1_doc, 't2': t2_doc}
					)
				return UnitResult(code=DocumentCode.T2_MISSING_INFO)

			if len(dps) != len(set(t1_dps_ids)):
				missing = sorted(
					set(t1_dps_ids) - {el['id'] for el in dps},
					key = t1_dps_ids.index
				)
				if not self._is_retriable(t2_doc, DocumentCode.T2_MISSING_INFO):
					report_error(
						self._ampel_db, msg='Some datapoints not found',
						logger=logger, info={'t1': t1_doc, 't2': t2_doc, 'missing': missing}
					)
				return UnitResult(code=DocumentCode.T2_MISSING_INFO)

			for dp in dps:
				if 'excl' in dp:
					del dp['excl'] # Optional channel based exclusion

			if isinstance(t2_unit, AbsTiedT2Unit):

				queries: list[dict[str, Any]] = []

				if isinstance(t2_unit, AbsTiedCustomStateT2Unit):
					# A LightCurve instance for example
					custom_state = t2_unit.build(t1_doc, dps)

				for tied_model in t2_unit.t2_dependency:

					d = self.build_tied_t2_query(t2_unit, tied_model, t2_doc)
					if d['link'] is None:

						if isinstance(tied_model, StateT2Dependency) and tied_model.link_override:

							filtr, sort, slc = tied_model.link_override.tools()
							l = dps

							if filtr:
								l = filtr.apply(l)

							# Sort (ex: by body.jd)
							if sort:
								l = sort(l)

							# Slice (ex: first datapoint)
							if slc:
								l = l[slc]

							d['link'] = maybe_match_array([el['id'] for el in l])

							if logger.verbose > 1:
								logger.debug(
									f"link_override result: {d['link']}",
									extra={'recipient': t2_doc['unit'], 'stock': t2_doc['stock']}
								)
						else:
							d['link'] = maybe_match_array(t1_dps_ids)

					queries.append(d)

				with stat_time.labels(t2_doc["unit"], "load_tied").time():
					qres = self.run_tied_queries(queries, t2_doc, stock_updr, logger)

				# Dependency missing
				if isinstance(qres, UnitResult):
					return qres

				if isinstance(t2_unit, AbsTiedStateT2Unit):
					return t1_doc, dps, qres

				# instance of AbsTiedCustomStateT2Unit
				return custom_state, qres

			else:

				if isinstance(t2_unit, AbsStateT2Unit):
					return t1_doc, dps

				else: # instance of AbsCustomStateT2Unit
					return (t2_unit.build(t1_doc, dps), )

		elif isinstance(t2_unit, AbsStockT2Unit):

			if doc := next(self.col_stock.find({'stock': t2_doc['link']}), None):
				return (doc, )

			if not self._is_retriable(t2_doc, DocumentCode.T2_UNKNOWN_LINK):
				report_error(
					self._ampel_db, msg='Stock doc not found',
					logger=logger, info={'doc': t2_doc}
				)
			return UnitResult(code=DocumentCode.T2_UNKNOWN_LINK)

		elif isinstance(t2_unit, (AbsPointT2Unit, AbsTiedPointT2Unit)):

			if doc := next(self.col_t0.find({'id': t2_doc['link']}), None):
				if isinstance(t2_unit, AbsTiedPointT2Unit):

					qres = self.run_tied_queries(
						[
							self.build_tied_t2_query(t2_unit, tied_model, t2_doc)
							for tied_model in t2_unit.t2_dependency
						],
						t2_doc, stock_updr, logger
					)

					if isinstance(qres, UnitResult):
						return qres

					return (doc, qres)

				return (doc, )

			if not self._is_retriable(t2_doc, DocumentCode.T2_UNKNOWN_LINK):
				report_error(
					self._ampel_db, msg='Datapoint not found',
					logger=logger, info={'doc': t2_doc}
				)
			return UnitResult(code=DocumentCode.T2_UNKNOWN_LINK)

		else:

			report_error(
				self._ampel_db, msg='Unknown T2 unit type',
				logger=logger, info={'doc': t2_doc}
			)

		return None


	def run_tied_queries(self,
		queries: list[dict[str, Any]],
		t2_doc: T2Document,
		stock_updr: MongoStockUpdater,
		logger: AmpelLogger
	) -> UnitResult | list[T2DocView]:

		t2_views: list[T2DocView] = []

		for query in queries:

			if self.run_dependent_t2s:

				processed_ids: list[ObjectId] = []

				# run pending dependencies
				while (
					dep_t2_doc := self.col.find_one_and_update(
						{
							'code': self.code_match if isinstance(self.code_match, DocumentCode)
							else maybe_match_array(self.code_match) # type: ignore[arg-type]
						} | query,
						{'$set': {'code': DocumentCode.RUNNING}}
					)
				):

					if logger.verbose > 1:
						logger.debug(
							'Processing tied t2 docs',
							extra={'unit': dep_t2_doc['unit'], 'stock': dep_t2_doc['stock']}
						)

					if not dep_t2_doc.get('body'):
						dep_t2_doc['body'] = []

					body, code = self.process_doc(dep_t2_doc, stock_updr, logger)
					dep_t2_doc['body'].append(body)
					dep_t2_doc['meta'].append({'code': code, 'tier': 2})

					# suppress channel info
					dep_t2_doc.pop('channel')
					t2_views.append(T2DocView.of(dep_t2_doc, self.context.config))
					processed_ids.append(dep_t2_doc['_id'])

				if len(processed_ids) > 0:
					query['_id'] = {'$nin': processed_ids}

			if logger.verbose > 1:
				logger.debug(
					'Running tied t2 query',
					extra={
						'unit': t2_doc['unit'],
						'stock': t2_doc['stock'],
						'query': convert_dollars(query)
					}
				)

			# collect dependencies
			for dep_t2_doc in self.col.find(query):
				# suppress channel info
				dep_t2_doc.pop('channel')
				t2_views.append(T2DocView.of(dep_t2_doc, self.context.config))

			for view in t2_views:
				if view.code in self._pending_codes:
					if logger.verbose > 1:
						logger.debug(
							'Dependent T2 unit not run yet',
							extra={
								'unit': view.unit,
								'stock': view.stock,
								'link': view.link,
								't2_type': view.t2_type,
							}
						)
					return UnitResult(code=DocumentCode.T2_PENDING_DEPENDENCY)
				elif view.get_payload() is None:
					logger.error(
						'Dependent T2 unit failed',
						extra={
							'unit': view.unit,
							'stock': view.stock,
							'link': view.link,
							't2_type': view.t2_type,
							'code': view.code,
						}
					)
					return UnitResult(code=DocumentCode.T2_FAILED_DEPENDENCY)

		if not t2_views:
			return UnitResult(code=DocumentCode.T2_MISSING_DEPENDENCY)

		return t2_views


	def build_tied_t2_query(self,
		t2_unit: AbsT2, tied_model: UnitModel, t2_doc: T2Document
	) -> dict[str, Any]:
		"""
		This method handles 'default' situations.
		Callers must check returned 'link'.
		If None (state t2 linked with point t2), further handling is required

		:raises:
			- ValueError if functionality is not implemented yet
			- BadConfig if what's requested is not possible (a point T2 cannot be linked with a state t2)
	
		:returns: link value to match or None if further handling is required (state t2 linked with point t2)
		"""

		t2_unit_info = self.context.config.get(
			f'unit.{tied_model.unit}', dict
		)

		if not t2_unit_info:
			raise ValueError(f'Unknown T2 unit {tied_model.unit}')

		d: dict[str, Any] = {
			'unit': tied_model.unit,
			'config': tied_model.config,
			'channel': {'$in': t2_doc['channel']},
			'stock': t2_doc['stock'],
		}

		if isinstance(t2_unit, AbsTiedPointT2Unit):

			if 'AbsPointT2Unit' in t2_unit_info['base']:
				d['link'] = t2_doc['link']

			elif 'AbsStockT2Unit' in t2_unit_info['base']:
				d['link'] = t2_doc['stock']

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
		t2_unit: AbsT2, t2_doc: T2Document, logger: AmpelLogger, stock_updr: MongoStockUpdater,
	) -> UBson | UnitResult:
		"""
		Regarding the possible int return code:
		usually, if an int is returned, it should be a DocumentCode member
		but let's not be too restrictive here
		"""

		args: Any = self.load_input_docs(t2_unit, t2_doc, logger, stock_updr)
		if args is None:
			logger.error("Unable to load information required to run t2 unit")
			return UnitResult(code=DocumentCode.INTERNAL_ERROR)

		if isinstance(args, UnitResult):
			return args

		try:

			with stat_time.labels(t2_doc["unit"], "process").time():
				ret = t2_unit.process(*args)

			if t2_unit._buf_hdlr.buffer: # type: ignore[union-attr]
				t2_unit._buf_hdlr.forward( # type: ignore[union-attr]
					logger, stock=t2_doc['stock']
				)

			return ret

		except Exception as e:

			if self.raise_exc:
				raise e

			# Record any uncaught exceptions in troubles collection.
			report_exception(
				self._ampel_db, logger, exc=e, info={
					'_id': t2_doc['_id'], # type: ignore
					'unit': t2_doc['unit'],
					'config': t2_doc['config'],
					'stock': t2_doc['stock'],
					'link': t2_doc['link'],
					'channel': t2_doc['channel']
				}
			)

			return UnitResult(code=DocumentCode.EXCEPTION)
