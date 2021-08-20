#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/t2/T2Worker.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 24.05.2019
# Last Modified Date: 09.08.2021
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from time import time
from bson import ObjectId
from typing import Optional, List, Union, Dict, Any, Sequence, Tuple, ClassVar, Literal

from ampel.types import T, UBson, ubson
from ampel.struct.UnitResult import UnitResult
from ampel.mongo.utils import maybe_match_array
from ampel.log.utils import convert_dollars
from ampel.enum.DocumentCode import DocumentCode
from ampel.content.StockDocument import StockDocument
from ampel.content.DataPoint import DataPoint
from ampel.content.T1Document import T1Document
from ampel.content.T2Document import T2Document
from ampel.view.T2DocView import T2DocView, TYPE_POINT_T2, TYPE_STOCK_T2, TYPE_STATE_T2
from ampel.log import AmpelLogger
from ampel.base.BadConfig import BadConfig
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
from ampel.model.UnitModel import UnitModel
from ampel.model.StateT2Dependency import StateT2Dependency
from ampel.core.StockJournalUpdater import StockJournalUpdater

AbsT2 = Union[
	AbsStockT2Unit, AbsPointT2Unit, AbsStateT2Unit, AbsTiedPointT2Unit,
	AbsTiedStateT2Unit, AbsCustomStateT2Unit[T], AbsTiedCustomStateT2Unit[T, U]
]

abs_t2 = (
	AbsStockT2Unit, AbsPointT2Unit, AbsStateT2Unit, AbsTiedPointT2Unit,
	AbsTiedStateT2Unit, AbsCustomStateT2Unit, AbsTiedCustomStateT2Unit
)

stat_latency, stat_count = register_stats(tier=2)


class T2Worker(AbsWorker[T2Document]):
	"""
	Fill results into pending :class:`T2 documents <ampel.content.T2Document.T2Document>`.
	"""

	#: process only those :class:`T2 documents <ampel.content.T2Document.T2Document>`
	#: with the given :attr:`~ampel.content.T2Document.T2Document.code`
	codes: Union[DocumentCode, Sequence[DocumentCode]] = [
		DocumentCode.NEW,
		DocumentCode.RERUN_REQUESTED,
		DocumentCode.T2_NEW_PRIO,
		DocumentCode.T2_PENDING_DEPENDENCY
	]

	run_dependent_t2s: bool = True
	tier: ClassVar[Literal[2]] = 2

	def process_doc(self,
		doc: T2Document,
		jupdr: StockJournalUpdater,
		logger: AmpelLogger
	) -> Tuple[UBson, int]:

		before_run = time()

		t2_unit = self.get_unit_instance(doc, logger)

		if not isinstance(t2_unit, abs_t2):
			raise ValueError(f"Unsupported unit: {doc['unit']}")

		if len(doc.get('body', [])) <= self.max_try:
			ret = self.run_t2_unit(t2_unit, doc, logger, jupdr)
		else:
			ret = UnitResult(code=DocumentCode.TOO_MANY_TRIALS)

		# Used as timestamp and to compute duration below (using before_run)
		now = time()

		# _id is an ObjectId, but declared as bytes in ampel-interface to avoid
		# an explicit dependency on pymongo
		stat_latency.labels(doc['unit']).observe(now - doc['meta'][0]['ts'])
		stat_count.labels(doc['unit']).inc()
		self._doc_counter += 1
		body = None
		code = 0

		try:

			# New (channel-less) journal entry for the associated stock document
			jrec = jupdr.add_record(
				stock = doc['stock'],
				doc_id = doc['_id'], # type: ignore
				trace_id = {'t2worker': self._trace_id, 't2unit': t2_unit._trace_id},
				unit = doc['unit']
			)

			# New t2 sub-result entry (later appended with additional values)
			meta = self.gen_meta(jupdr.run_id, t2_unit._trace_id, round(now - before_run, 3))

			# Unit requested customizations
			if isinstance(ret, UnitResult):

				if ret.body:
					body = ret.body

				if ret.code:
					code = ret.code

				# TODO: check that unit did not use system reserved code
				if code != 0 and code in DocumentCode.__members__.values():
					logger.info(f'T2 unit {t2_unit.__class__.__name__} returned document code: {code} ({DocumentCode(code).name})')

				if ret.journal:
					jrec.update(ret.journal) # type: ignore

			# Unit returned bson-like content
			elif isinstance(ret, ubson):
				body = ret

			# Unsupported object returned by unit
			else:
				code = DocumentCode.ERROR
				self._processing_error(
					logger, doc, None, jrec, meta, extra={'ret': ret},
					msg='Invalid content returned by T2 unit'
				)

			meta['code'] = code
			self.commit_update(
				{'_id': doc['_id']}, # type: ignore[typeddict-item]
				meta, logger, body=body, code=code
			)

			# Update stock document
			jupdr.flush()

		except Exception as e:

			if self.raise_exc:
				raise e

			self._processing_error(
				logger, doc, None, jrec, exception=e, msg='An exception occured',
				meta = self.gen_meta(jupdr.run_id, t2_unit._trace_id, round(now - before_run, 3))
			)

		return body, code


	# NB: spell out union arg to ensure a common context for the TypeVar T
	def load_input_docs(self,
		t2_unit: AbsT2, t2_doc: T2Document, logger: AmpelLogger, jupdater: StockJournalUpdater
	) -> Union[
		None,
		UnitResult,                                                # Error / missing dependency
		DataPoint,                                               # point t2
		Tuple[DataPoint, T2DocView],                             # tied point t2
		Tuple[StockDocument],                                    # stock t2
		Tuple[T1Document, Sequence[DataPoint]],                  # state t2
		Tuple[T],                                                # custom state t2 (T could be LightCurve)
		Tuple[T1Document, Sequence[DataPoint], List[T2DocView]], # tied state t2
		Tuple[T, List[T2DocView]],                               # tied custom state t2
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
			dps: List[DataPoint] = []
			t1_doc: Optional[T1Document] = next(self.col_t1.find({'link': t2_doc['link']}), None)

			# compound doc must exist (None could mean an ingester bug)
			if t1_doc is None:
				report_error(
					self._ampel_db, msg='T1Document not found', logger=logger,
					info={'id': t2_doc['link'], 'doc': t2_doc}
				)
				return None

			# Datarights: suppress channel info (T3 uses instead a
			# 'projection' procedure that should not be necessary here)
			t1_doc.pop('channel')

			t1_dps_ids = list(t1_doc['dps'])
			dps = sorted(
				self.col_t0.find({'id': {'$in': t1_dps_ids}}),
				key = lambda dp: t1_dps_ids.index(dp['id'])
			)

			# Should never happen (only in case of ingestion bug)
			if not dps:
				report_error(
					self._ampel_db, msg='Datapoints not found',
					logger=logger, info={'t1': t1_doc, 't2': t2_doc}
				)
				return None

			if len(dps) != len(t1_dps_ids):
				for el in (set(t1_dps_ids) - {el['id'] for el in dps}):
					logger.error(f'Datapoint {el} referenced in compound not found')
				return None

			for dp in dps:
				if 'excl' in dp:
					del dp['excl'] # Optional channel based exclusion

			if isinstance(t2_unit, AbsTiedT2Unit):

				queries: List[Dict[str, Any]] = []

				if isinstance(t2_unit, AbsTiedCustomStateT2Unit):
					# A LightCurve instance for example
					custom_state = t2_unit.build(t1_doc, dps)

				for tied_model in t2_unit.t2_dependency:

					d = self.build_tied_t2_query(t2_unit, tied_model, t2_doc)
					if d['link'] is None:

						if isinstance(tied_model, StateT2Dependency) and tied_model.link_override:
							if isinstance(t2_unit, AbsTiedCustomStateT2Unit):
								d['link'] = t2_unit.get_link(tied_model.link_override, custom_state)
							else:
								d['link'] = t2_unit.get_link( # type: ignore[union-attr] # mypy forgot the first 'if' of this method
									tied_model.link_override, t1_doc, dps
								)
							if logger.verbose > 1:
								logger.debug(f"get_link() value: {d['link']}")
						else:
							d['link'] = maybe_match_array(t1_dps_ids)

					queries.append(d)

				qres = self.run_tied_queries(queries, t2_doc, jupdater, logger)

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

			report_error(
				self._ampel_db, msg='Stock doc not found',
				logger=logger, info={'doc': t2_doc}
			)

		elif isinstance(t2_unit, (AbsPointT2Unit, AbsTiedPointT2Unit)):

			if doc := next(self.col_t0.find({'id': t2_doc['link']}), None):
				if isinstance(t2_unit, AbsTiedPointT2Unit):

					qres = self.run_tied_queries(
						[
							self.build_tied_t2_query(t2_unit, tied_model, t2_doc)
							for tied_model in t2_unit.t2_dependency
						],
						t2_doc, jupdater, logger
					)

					if isinstance(qres, UnitResult):
						return qres

					return (doc, qres)

				return (doc, )

			report_error(
				self._ampel_db, msg='Datapoint not found',
				logger=logger, info={'doc': t2_doc}
			)

		else:

			report_error(
				self._ampel_db, msg='Unknown T2 unit type',
				logger=logger, info={'doc': t2_doc}
			)

		return None


	def run_tied_queries(self,
		queries: List[Dict[str, Any]],
		t2_doc: T2Document,
		jupdater: StockJournalUpdater,
		logger: AmpelLogger
	) -> Union[UnitResult, List[T2DocView]]:

		t2_views: List[T2DocView] = []

		for query in queries:

			if self.run_dependent_t2s:

				processed_ids: List[ObjectId] = []

				# run pending dependencies
				while (
					dep_t2_doc := self.col.find_one_and_update(
						{'code': maybe_match_array(self.code_match), **query}, # type: ignore[arg-type]
						{'$set': {'code': DocumentCode.RUNNING}}
					)
				):

					#if logger.verbose > 1:
					logger.debug('Processing tied t2 docs', extra={'unit': dep_t2_doc['unit']})

					if not dep_t2_doc.get('body'):
						dep_t2_doc['body'] = []

					body, code = self.process_doc(dep_t2_doc, jupdater, logger)
					dep_t2_doc['body'].append(body)
					dep_t2_doc['meta'].append({'code': code, 'tier': 2})

					# suppress channel info
					dep_t2_doc.pop('channel')
					t2_views.append(self.view_from_doc(dep_t2_doc))
					processed_ids.append(dep_t2_doc['_id'])

				if len(processed_ids) > 0:
					query['_id'] = {'$nin': processed_ids}

			if logger.verbose > 1:
				logger.debug('Running tied t2 query', extra={'query': convert_dollars(query)})

			# collect dependencies
			for dep_t2_doc in self.col.find(query):
				# suppress channel info
				dep_t2_doc.pop('channel')
				t2_views.append(self.view_from_doc(dep_t2_doc))

			for view in t2_views:
				if view.get_data() is None:
					if logger.verbose > 1:
						logger.debug(
							'Dependent T2 unit not run yet',
							extra={'t2_oid': t2_doc['_id']} # type: ignore
						)
					return UnitResult(code=DocumentCode.T2_PENDING_DEPENDENCY)

		if not t2_views:
			return UnitResult(code=DocumentCode.T2_MISSING_DEPENDENCY)

		return t2_views


	def view_from_doc(self, doc: T2Document) -> T2DocView:
		"""
		We might want to move this method elsewhere in the future
		"""

		t2_unit_info = self.context.config.get(f'unit.{doc["unit"]}', dict)
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
			t2_type = t2_type,
			link = doc['link'],
			code = doc['code'],
			meta = doc['meta'],
			body = doc.get('body'),
			created = doc['_id'].generation_time.timestamp(), # type: ignore
			config = self.context.config.get(f'confid.{doc["config"]}', dict) \
				if doc['config'] else None
		)


	def build_tied_t2_query(self,
		t2_unit: AbsT2, tied_model: UnitModel, t2_doc: T2Document
	) -> Dict[str, Any]:
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

		d: Dict[str, Any] = {
			'unit': tied_model.unit,
			'config': tied_model.config,
			'channel': {'$in': t2_doc['channel']},
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
		t2_unit: AbsT2, t2_doc: T2Document, logger: AmpelLogger, jupdater: StockJournalUpdater,
	) -> Union[UBson, UnitResult]:
		"""
		Regarding the possible int return code:
		usually, if an int is returned, it should be a DocumentCode member
		but let's not be too restrictive here
		"""

		args: Any = self.load_input_docs(t2_unit, t2_doc, logger, jupdater)
		if args is None:
			return UnitResult(code=DocumentCode.ERROR)

		if isinstance(args, UnitResult):
			return args

		try:

			ret = t2_unit.process(*args)

			if t2_unit._buf_hdlr.buffer: # type: ignore[union-attr]
				t2_unit._buf_hdlr.forward( # type: ignore[union-attr]
					logger, stock=t2_doc['stock'], channel=t2_doc['channel']
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
					'channel': t2_doc['channel'],
				}
			)

			return UnitResult(code=DocumentCode.EXCEPTION)
