#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/t3/T3Processor.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 26.02.2018
# Last Modified Date: 18.04.2021
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from contextlib import contextmanager
from typing import Dict, Any, List, Optional, Union, Sequence, Generator
from ampel.abstract.AbsProcessorUnit import AbsProcessorUnit
from ampel.util.collections import try_reduce, chunks as chunks_func
from ampel.model.UnitModel import UnitModel
from ampel.content.T3Document import T3Document
from ampel.struct.AmpelBuffer import AmpelBuffer
from ampel.log.utils import report_exception
from ampel.log import AmpelLogger, LogFlag, SHOUT
from ampel.core.EventHandler import EventHandler
from ampel.core.StockJournalUpdater import StockJournalUpdater
from ampel.t3.session.AbsT3SessionInfo import AbsT3SessionInfo
from ampel.t3.select.AbsT3Selector import AbsT3Selector
from ampel.t3.load.AbsT3Loader import AbsT3Loader
from ampel.t3.complement.AbsT3DataAppender import AbsT3DataAppender
from ampel.t3.stage.AbsT3Stager import AbsT3Stager


class T3Processor(AbsProcessorUnit):
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

	#: Provides contextual informations to this run (fills session_info of T3 units)
	session: Optional[Sequence[UnitModel]]

	#: Select stocks
	select: Optional[UnitModel]

	#: Fill :class:`~ampel.core.AmpelBuffer.AmpelBuffer` for each selected stock
	load: Optional[UnitModel]

	#: Add external information to each :class:`~ampel.core.AmpelBuffer.AmpelBuffer`.
	complement: Optional[Sequence[UnitModel]]

	#: Execute provided stager unit (which in turns provides views to underlying T3 unit(s))
	stage: UnitModel

	#: number of stocks to load at once. Set to 0 to disable chunking
	chunk_size: int = 1000

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

		# Admins have the option to to run a T3 process silently/anonymously
		if self.update_events:

			if not self.process_name:
				raise ValueError('Parameter process_name must be defined')

			# Create event doc
			event_hdlr = EventHandler(
				self.context.db, tier=3, run_id=run_id,
				process_name=self.process_name,
			)

		with self.error_handling(logger, event_hdlr):

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
						.new_admin_unit(
							unit_model = el,
							context = self.context,
							sub_type = AbsT3SessionInfo,
							logger = logger,
							process_name = self.process_name
						) \
						.update(session_info)


			jupdater = StockJournalUpdater(
				ampel_db = self.context.db, tier = 3, run_id = run_id,
				process_name = self.process_name, logger = logger,
				raise_exc = self.raise_exc, extra_tag = self.extra_journal_tag,
				update_journal = self.update_journal,
				update_modified = False
			)

			# Stager unit
			#############

			# pyampel-core provides 3 stager implementations: T3SimpleStager, T3ProjectingStager, T3DynamicStager
			stager = self.context.loader \
				.new_admin_unit(
					unit_model = self.stage,
					context = self.context,
					sub_type = AbsT3Stager,
					logger = logger,
					jupdater = jupdater,
					channel = self.stage.config['channel'] if self.stage.config.get('channel') else self.channel, # type: ignore
					session_info = session_info
				)

			# target selection
			##################

			if self.select:

				# Spawn and run a new selector instance
				# stock_ids is an iterable (often a pymongo cursor)
				selector = self.context.loader.new_admin_unit(
					unit_model = self.select,
					context = self.context,
					sub_type = AbsT3Selector,
					logger = logger
				)

				# Content loader
				################

				if self.load:
					# Spawn requested content loader
					data_loader = self.context.loader \
						.new_admin_unit(
							unit_model = self.load,
							context = self.context,
							sub_type = AbsT3Loader,
							logger = logger
						)


				# Content complementer
				######################

				# Spawn potentialy requested snapdata complementers
				if self.complement:
					complementers: Optional[List[AbsT3DataAppender]] = [
						self.context.loader \
							.new_admin_unit(
								unit_model = conf_el,
								context = self.context,
								sub_type = AbsT3DataAppender,
								logger = logger
							)
						for conf_el in self.complement
					]
				else:
					complementers = None

				# NB: we consume the entire stock selection cursor at once using list()
				# to be robust against cursor timeouts or server restarts during long-
				# lived T3 processes
				if stock_ids := list(selector.fetch() or []):

					if (
						t3_records := stager.stage(
							self.generate_buffer(
								selector, data_loader, stager, logger, event_hdlr, complementers
							)
						)
					):

						# TODO: implement something for setting version & config
						d = T3Document(
							process = self.process_name,
							code = stager.get_codes(),
							run = run_id
						)

						if self.channel:
							d['channel'] = self.channel

						if x := stager.get_tags():
							d['tag'] = x

						if self.save_stock_ids:
							d['stock'] = [el[selector.field_name] for el in stock_ids]

						d['body'] = try_reduce(t3_records)

						self.context.db.get_collection('t3').insert_one(d)


		if not logger:
			logger = AmpelLogger.get_logger()

		# Feedback
		logger.log(SHOUT, f'Done running {self.process_name}')
		logger.flush()

		# Update event document
		if event_hdlr:
			event_hdlr.update(logger)


	def generate_buffer(self,
		selector: AbsT3Selector,
		data_loader: AbsT3Loader,
		stager: AbsT3Stager,
		logger: Optional[AmpelLogger] = None,
		event_hdlr: Optional[EventHandler] = None,
		complementers: Optional[List[AbsT3DataAppender]] = None
	) -> Generator[AmpelBuffer, None, None]:


		stock_ids = list(selector.fetch() or [])
		if not stock_ids:
			raise StopIteration

		# Usually, id_key is '_id' but it can be 'stock' if the
		# selection is based on t2 documents for example
		id_key = selector.field_name

		# Run start
		###########
		chunks = chunks_func(stock_ids, self.chunk_size) if self.chunk_size > 0 else [stock_ids]

		# Loop over chunks from the cursor/iterator
		for chunk_ids in chunks:

			# allow working chunks to complete even if some raise exception
			with self.error_handling(logger, event_hdlr):

				# Load info from DB
				tran_data = data_loader.load([sid[id_key] for sid in chunk_ids])

				# Potentialy add complementary information (spectra, TNS names, ...)
				if complementers:
					for appender in complementers:
						appender.complement(tran_data)

				for ampel_buffer in tran_data:
					yield ampel_buffer


	@contextmanager
	def error_handling(self,
		logger: Optional[AmpelLogger] = None,
		event_hdlr: Optional[EventHandler] = None
	):

		try:
			yield

		except Exception as e:

			if not logger:
				logger = AmpelLogger.get_logger()

			if event_hdlr:
				event_hdlr.add_extra(logger, success=False)

			if self.raise_exc:
				raise e

			report_exception(
				self.context.db, logger, exc=e,
				info={'process': self.process_name}
			)
