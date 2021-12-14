#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/t3/T3Processor.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 26.02.2018
# Last Modified Date: 12.12.2021
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from pydantic import BaseModel
from typing import Any, Optional, Union, Sequence
from ampel.abstract.AbsEventUnit import AbsEventUnit
from ampel.abstract.AbsT3ControlUnit import AbsT3ControlUnit
from ampel.t3.T3PlainUnitExecutor import T3PlainUnitExecutor
from ampel.t3.T3ReviewUnitExecutor import T3ReviewUnitExecutor
from ampel.view.T3Store import T3Store
from ampel.view.T3DocView import T3DocView
from ampel.model.UnitModel import UnitModel
from ampel.core.EventHandler import EventHandler
from ampel.mongo.update.MongoStockUpdater import MongoStockUpdater
from ampel.abstract.AbsT3Supplier import AbsT3Supplier
from ampel.log import AmpelLogger, LogFlag, SHOUT


class IncludeModel(BaseModel):
	"""
	:param session: models for AbsT3Supplier[dict] instances which populates the 'session' field of T3Store
	Examples of session information are:
	- Date and time the current process was last run
	- Number of alerts processed since then
	"""

	#: Provides Iterable[T3Document]
	docs: Optional[UnitModel]

	#: Provides session information. Unit(s) must be a subclass of AbsT3Supplier
	session: Union[None, Sequence[UnitModel], UnitModel]


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

	include: Optional[IncludeModel]

	execute: Sequence[dict[str, Any]] # to be improved later


	def __init__(self,
		update_journal: bool = True,
		update_events: bool = True,
		extra_journal_tag: Optional[Union[int, str]] = None,
		**kwargs
	) -> None:
		"""
		Note that update_journal, update_event and extra_journal_tag are admin run options and
		should be set only on command line. They are thus not defined as part of the underlying model.
		Please see class docstring for more info.
		"""

		self.react_first = True
		for k in kwargs.keys():
			if k == "react":
				break
			if k == "act":
				self.react_first = False
				break

		super().__init__(**kwargs)

		self.update_journal = update_journal

		# Admins have the option to run a T3 process silently/anonymously
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

		# Create event doc
		event_hdlr = EventHandler(
			self.process_name, self.context.db, tier=3, run_id=run_id,
			raise_exc=self.raise_exc, dry_run=not self.update_events
		)

		try:

			# Feedback
			logger.log(SHOUT, f'Running {self.process_name}')

			loader = self.context.loader
			t3s = T3Store()

			if self.include:

				if self.include.docs:
					pass # later

				if (x := self.include.session):
					sdict: dict[str, Any] = {}
					for um in [x] if isinstance(x, UnitModel) else x:
						rd = loader.new_context_unit(
							model = um,
							context = self.context,
							sub_type = AbsT3Supplier,
							event_hdlr = event_hdlr,
							logger = logger
						).supply(t3s)
						if rd:
							sdict |= rd
					if sdict:
						t3s.add_session_info(sdict)

			stock_updr = MongoStockUpdater(
				ampel_db = self.context.db, tier = 3, run_id = run_id,
				process_name = self.process_name, logger = logger,
				raise_exc = self.raise_exc, extra_tag = self.extra_journal_tag,
				update_journal = self.update_journal,
				bump_updated = False
			)

			for i, d in enumerate(self.execute):

				if 'unit' in d:

					# Native AbsT3ControlUnit
					if "ContextUnit" in self.context.config.get(f"unit.{d['unit']}.base", tuple, raise_exc=True): # type: ignore
						t3_unit = loader.new_context_unit(
							model = UnitModel(unit=d['unit'], config=d['config']),
							context = self.context,
							sub_type = AbsT3ControlUnit,
							logger = logger
						)

					# AbsT3PlainUnit run by AbsT3ControlUnit
					else:
						# Dedicated T3ControlUnit
						t3_unit = T3PlainUnitExecutor(
							unit = d['unit'],
							config = d['config'],
							context = self.context,
							logger = logger,
							event_hdlr = event_hdlr,
							stock_updr = stock_updr,
							channel = self.channel
						)

				# AbsT3ReviewUnit run by AbsT3ControlUnit
				else:

					t3_unit = T3ReviewUnitExecutor(
						logger = logger,
						context = self.context,
						event_hdlr = event_hdlr,
						stock_updr = stock_updr,
						channel = self.channel,
						**d
					)

				logger.info(f"Processing run block {i}", extra={'unit': t3_unit.__class__.__name__})

				# Potential T3Document to be included in the t3 collection
				if (ret := t3_unit.process(t3s)):
					for t3d in ret:
						t3s.add_view(T3DocView.of(t3d, self.context.config))
						if 'meta' not in t3d:
							raise ValueError("Invalid T3Document")
						t3d['meta']['traceid'] = {'t3processor': self._trace_id}
						self.context.db.get_collection('t3').insert_one(t3d)


		except Exception as e:
			event_hdlr.handle_error(e, logger)

		# Update event document
		if event_hdlr:
			event_hdlr.add_extra(success=True)
			event_hdlr.update(logger)

		# Feedback
		logger.log(SHOUT, f'Done running {self.process_name}')
		logger.flush()
