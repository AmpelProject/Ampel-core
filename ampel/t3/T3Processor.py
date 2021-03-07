#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/t3/T3Processor.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 26.02.2018
# Last Modified Date: 07.03.2021
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Dict, Any, List, Optional, Union

from ampel.abstract.AbsProcessorUnit import AbsProcessorUnit
from ampel.log import AmpelLogger, LogFlag, SHOUT
from ampel.core.EventHandler import EventHandler
from ampel.log.utils import report_exception
from ampel.content.T3Document import T3Document
from ampel.model.t3.T3Directive import T3Directive
from ampel.enum.T3ProcessorStatus import T3ProcessorStatus
from ampel.t3.load.AbsT3Loader import AbsT3Loader
from ampel.t3.run.AbsT3UnitRunner import AbsT3UnitRunner
from ampel.t3.select.AbsT3Selector import AbsT3Selector
from ampel.t3.complement.AbsT3DataAppender import AbsT3DataAppender
from ampel.t3.context.AbsT3RunContextAppender import AbsT3RunContextAppender
from ampel.util.collections import chunks


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

	#: T3 processing schema
	directive: T3Directive

	#: number of stocks to load at once
	chunk_size: int = 200

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

		if 'db' not in self.context.config.get(f"logging.{self.log_profile}", dict, raise_exc=True):
			for el in ("update_journal", "update_events"):
				if getattr(self, el):
					raise ValueError(
						f"{el} cannot be True without a logger associated with a db logging handler"
					)


	def run(self) -> None:

		event_hdlr = None
		run_id = self.new_run_id()
		exc = None
		logger = None

		try:

			logger = AmpelLogger.from_profile(
				self.context, self.log_profile, run_id,
				base_flag = LogFlag.T3 | LogFlag.CORE | self.base_log_flag,
				force_refresh = True
			)

			# Admins have the option to to run a T3 process silently/anonymously
			if self.update_events:

				if not self.process_name:
					raise ValueError("Parameter process_name must be defined")

				# Create event doc
				event_hdlr = EventHandler(
					self.context.db, tier=3, run_id=run_id,
					process_name=self.process_name,
				)

			# Feedback
			logger.log(SHOUT, f"Running {self.process_name}")

			# run context
			#############

			run_context: Dict[str, Any] = {}

			if self.context.admin_msg:
				run_context['admin_msg'] = self.context.admin_msg

			if self.directive.context:

				for el in self.directive.context:
					self.context.loader \
						.new_admin_unit(
							unit_model = el,
							context = self.context,
							sub_type = AbsT3RunContextAppender,
							logger = logger,
							process_name = self.process_name
						) \
						.update(run_context)

			# Unit runner
			#############

			chan = self.directive.run.config['channel'] if 'channel' in self.directive.run.config else self.channel # type: ignore

			# The default runner provided by pyampel-core is T3UnitRunner
			runner = self.context.loader \
				.new_admin_unit(
					unit_model = self.directive.run,
					context = self.context,
					sub_type = AbsT3UnitRunner,
					logger = logger,
					run_id = run_id,
					process_name = self.process_name,
					channel = chan,
					update_journal = self.update_journal,
					extra_journal_tag = self.extra_journal_tag,
					run_context = run_context
				)

			# target selection
			##################

			if self.directive.select:

				# Spawn and run a new selector instance
				# stock_ids is an iterable (often a pymongo cursor)
				selector = self.context.loader.new_admin_unit(
					unit_model = self.directive.select,
					context = self.context,
					sub_type = AbsT3Selector,
					logger = logger
				)

				# Usually, id_key is '_id' but it can be 'stock' if the
				# selection is based on t2 documents for example
				id_key = selector.field_name


				# Content loader
				################

				if self.directive.load:
					# Spawn requested content loader
					content_loader = self.context.loader \
						.new_admin_unit(
							unit_model = self.directive.load,
							context = self.context,
							sub_type = AbsT3Loader,
							logger = logger
						)


				# Content complementer
				######################

				# Spawn potentialy requested snapdata complementers
				if self.directive.complement:
					comps: List[AbsT3DataAppender] = [
						self.context.loader \
							.new_admin_unit(
								unit_model = conf_el,
								context = self.context,
								sub_type = AbsT3DataAppender,
								logger = logger
							)
						for conf_el in self.directive.complement
					]

				# NB: we consume the entire stock selection cursor at once using list()
				# to be robust against cursor timeouts or server restarts during long-
				# lived T3 processes
				if stock_ids := list(selector.fetch() or []):

					# Run start
					###########

					# Loop over chunks from the cursor/iterator

					for chunk_ids in chunks(stock_ids, self.chunk_size):

						# try/catch here to allow some chunks to complete
						# even if one raises an exception
						try:
							# Load info from DB
							tran_data = content_loader.load([sid[id_key] for sid in chunk_ids])

							# Potentialy add complementary information (spectra, TNS names, ...)
							if self.directive.complement:
								for appender in comps:
									appender.complement(tran_data)

							# Run T3 units defined for this process
							runner.run(list(tran_data))

						except Exception as e:
							exc = e
							if self.raise_exc:
								raise e

							report_exception(
								self.context.db, logger, exc=e,
								info={'process': self.process_name}
							)

			if (t3_records := runner.done()):

				# TODO: implement something for setting version & config
				d = T3Document(
					process = self.process_name,
					run = run_id,
					status = T3ProcessorStatus.OK
				)

				if self.channel:
					d['channel'] = self.channel

				if self.save_stock_ids:
					d['stock'] = [el['_id'] for el in stock_ids]

				# Mongo maintains key order
				d['body'] = [t3_records] if isinstance(t3_records, dict) else t3_records

				self.context.db.get_collection('t3').insert_one(d)

		except Exception as e:

			exc = e
			if self.raise_exc:
				raise e

			if not logger:
				logger = AmpelLogger.get_logger()

			report_exception(
				self.context.db, logger, exc=e,
				info={'process': self.process_name}
			)

		finally:

			if not logger:
				logger = AmpelLogger.get_logger()

			# Feedback
			logger.log(SHOUT, f"Done running {self.process_name}")
			logger.flush()

			# Register the execution of this event into the events col
			if event_hdlr:
				event_hdlr.update(logger, success=(exc is None))
