#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/t3/T3Processor.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 26.02.2018
# Last Modified Date: 31.08.2021
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Dict, Any, Optional, Union
from ampel.abstract.AbsEventUnit import AbsEventUnit
from ampel.model.UnitModel import UnitModel
from ampel.log.utils import report_exception
from ampel.log import AmpelLogger, LogFlag, SHOUT
from ampel.core.EventHandler import EventHandler
from ampel.mongo.update.MongoStockUpdater import MongoStockUpdater
from ampel.abstract.AbsSessionInfo import AbsSessionInfo
from ampel.abstract.AbsT3Supplier import AbsT3Supplier
from ampel.abstract.AbsT3Stager import AbsT3Stager


class T3Processor(AbsEventUnit):
	"""
	:param update_journal: Record the invocation of this event in the stock journal
	:param update_events: Record this event in the events collection
	:param extra_journal_tag:
	  tag(s) to add to the :class:`~ampel.content.JournalRecord.JournalRecord`
	  of each selected stock

	.. note:: by default, :func:`run` add entries to the journals of selected
	  stocks, store log records in the database, and records the invocation in
	  the events collection. To run an "anonymous" event that does not change
	  the state of the database:
	
	  - use a log_profile without db logging (profiles are defined under 'logging' in the ampel config)
	  - set update_journal = False (no update to the transient doc)
	  - set update_events = False (no update to the events collection)
	  - set raise_exc = True (troubles collection will not be populated if an exception occurs)
	"""

	#: Provides :class:`~ampel.core.AmpelBuffer.AmpelBuffer` generator
	supply: UnitModel

	#: Execute provided stager unit (which in turns provides views to underlying T3 unit(s))
	stage: UnitModel

	#: whether selected stock ids should be saved into (potential) t3 documents
	save_stock_ids: bool = True


	def __init__(self,
		update_journal: bool = True,
		update_events: bool = True,
		extra_journal_tag: Optional[Union[int, str]] = None,
		**kwargs
	) -> None:
		"""
		Note that update_journal, update_event and extra_journal_tag are admin run options and
		should be set only on command line. They are thus not defined as part of the underlying model.
		"""

		super().__init__(**kwargs)

		self.update_journal = update_journal
		self.update_events = update_events
		self.extra_journal_tag = extra_journal_tag

		if 'db' not in self.context.config.get(f'logging.{self.log_profile}', dict, raise_exc=True):
			for el in ('update_journal', 'update_events'):
				if getattr(self, el):
					raise ValueError(
						f'{el} cannot be True without a logger associated with a db logging handler'
					)


	def run(self) -> None:

		event_hdlr = None
		run_id = self.context.new_run_id()

		logger = AmpelLogger.from_profile(
			self.context, self.log_profile, run_id,
			base_flag = LogFlag.T3 | LogFlag.CORE | self.base_log_flag,
			force_refresh = True
		)

		# Admins have the option to run a T3 process silently/anonymously
		if self.update_events:

			if not self.process_name:
				raise ValueError('Parameter process_name must be defined')

			# Create event doc
			event_hdlr = EventHandler(
				self.context.db, tier=3, run_id=run_id,
				process_name=self.process_name,
			)

		try:

			# Feedback
			logger.log(SHOUT, f'Running {self.process_name}')

			# run context
			#############

			session_info: Dict[str, Any] = {}
			if self.context.admin_msg:
				session_info['admin_msg'] = self.context.admin_msg

			if self.session:

				for el in self.session:
					self.context.loader \
						.new_context_unit(
							model = el,
							context = self.context,
							sub_type = AbsSessionInfo,
							logger = logger,
							process_name = self.process_name,
							_provenance = False
						) \
						.update(session_info)


			stock_updr = MongoStockUpdater(
				ampel_db = self.context.db, tier = 3, run_id = run_id,
				process_name = self.process_name, logger = logger,
				raise_exc = self.raise_exc, extra_tag = self.extra_journal_tag,
				update_journal = self.update_journal,
				bump_updated = False
			)

			# Stager unit
			#############

			# pyampel-core provides 3 stager implementations: T3SimpleStager, T3ProjectingStager, T3DynamicStager
			stager = self.context.loader \
				.new_context_unit(
					model = self.stage,
					context = self.context,
					sub_type = AbsT3Stager,
					logger = logger,
					stock_updr = stock_updr,
					channel = self.stage.config['channel'] if self.stage.config.get('channel') else self.channel, # type: ignore
					session_info = session_info,
					_provenance = False
				)

			# Spawn and run a new selector instance
			# stock_ids is an iterable (often a pymongo cursor)
			supplier = self.context.loader.new_context_unit(
				model = self.supply,
				context = self.context,
				sub_type = AbsT3Supplier,
				logger = logger,
				event_hdlr = event_hdlr,
				raise_exc = self.raise_exc,
				process_name = self.process_name,
				_provenance = False
			)

			if t3_docs := stager.stage(
				supplier.supply()
			):
				for t3d in t3_docs if isinstance(t3_docs, list) else [t3_docs]:

					# 0: no provenance. -1: args not serializable
					if self._trace_id not in (0, -1):
						if 'meta' not in t3d:
							t3d['meta'] = {}
						t3d['meta']['traceid'] = {'t3processor': self._trace_id}

					self.context.db \
						.get_collection('t3') \
						.insert_one(t3d)

		except Exception as e:

			if event_hdlr:
				event_hdlr.add_extra(overwrite=True, success=False)

			if self.raise_exc:
				raise e

			report_exception(
				self.context.db, logger, exc=e,
				info={'process': self.process_name}
			)
		# Feedback
		logger.log(SHOUT, f'Done running {self.process_name}')
		logger.flush()

		# Update event document
		if event_hdlr:
			event_hdlr.add_extra(success=True)
			event_hdlr.update(logger)
