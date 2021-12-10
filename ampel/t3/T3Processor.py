#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/t3/T3Processor.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 26.02.2018
# Last Modified Date: 10.12.2021
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from time import time
from pydantic import BaseModel
from typing import Any, Optional, Union, Sequence, Type
from ampel.types import ChannelId
from ampel.abstract.AbsEventUnit import AbsEventUnit
from ampel.abstract.AbsT3ControlUnit import AbsT3ControlUnit
from ampel.abstract.AbsT3PlainUnit import AbsT3PlainUnit
from ampel.t3.T3Writer import T3Writer
from ampel.view.T3Store import T3Store
from ampel.view.T3DocView import T3DocView
from ampel.model.UnitModel import UnitModel
from ampel.core.UnitLoader import UnitLoader, LT
from ampel.core.EventHandler import EventHandler
from ampel.mongo.update.MongoStockUpdater import MongoStockUpdater
from ampel.abstract.AbsT3Supplier import AbsT3Supplier
from ampel.abstract.AbsT3Stager import AbsT3Stager
from ampel.log.utils import report_exception
from ampel.log import AmpelLogger, LogFlag, SHOUT, VERBOSE
from ampel.log.handlers.ChanRecordBufHandler import ChanRecordBufHandler
from ampel.log.handlers.DefaultRecordBufferingHandler import DefaultRecordBufferingHandler


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
	session: Union[None, UnitModel, Sequence[UnitModel]]


class ReactModel(BaseModel):

	#: Unit must be a subclass of AbsT3Supplier
	supply: UnitModel

	#: Unit must be a subclass of AbsT3Stager
	stage: UnitModel


class T3Executor(T3Writer):
	execute: Union[Sequence[UnitModel], UnitModel]


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

	react: Optional[ReactModel]

	act: Optional[Any]


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

			loader = self.context.loader
			t3s = T3Store()

			if self.include:

				if self.include.docs:
					pass

				if (x := self.include.session):
					sdict: dict[str, Any] = {}
					for el in [x] if isinstance(x, UnitModel) else x:
						rd = loader.new_context_unit(
							model = el,
							context = self.context,
							sub_type = AbsT3Supplier,
							logger = logger,
							process_name = self.process_name,
							#_provenance = False
						).supply()
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

			if self.react_first and self.react:
				self._react(t3s, stock_updr, event_hdlr, logger)

			if self.act:

				t3e = T3Executor(
					context = self.context,
					logger = logger,
					stock_updr = stock_updr,
					process_name = self.process_name,
					**self.act
				)

				t3_units: list[Union[AbsT3ControlUnit, AbsT3PlainUnit]] = []

				for um in [t3e.execute] if isinstance(t3e.execute, UnitModel) else t3e.execute:
					if "ContextUnit" in self.context.config.get(f"unit.{um.unit}.base", tuple, raise_exc=True): # type: ignore
						t3_units.append(
							loader.new_context_unit(
								model = um,
								context = self.context,
								sub_type = AbsT3ControlUnit,
								logger = logger
							)
						)
					else:
						t3_units.append(
							self.spawn_logical_unit(
								um,
								unit_type = AbsT3PlainUnit,
								loader = loader,
								logger = logger,
								chan = self.channel
							)
						)
				
				for t3_unit in t3_units:

					logger.info("Running unit", extra={'unit': t3_unit.__class__.__name__})
						
					# potential T3Document to be included in the T3Document
					if (ret := t3_unit.process(t3s)):
						if doc := t3e.handle_t3_result(t3_unit, ret, None, time()):
							t3s.add_view(T3DocView.of(doc, self.context.config))
							doc['meta']['traceid'] = {'t3processor': self._trace_id}
							self.context.db.get_collection('t3').insert_one(doc)

					t3e.flush(t3_unit)

			if self.react and not self.react_first:
				self._react(t3s, stock_updr, event_hdlr, logger)

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


	def _react(self,
		t3s: T3Store,
		stock_updr: MongoStockUpdater,
		event_hdlr: Optional[EventHandler],
		logger: AmpelLogger
	) -> None:
		
		if not self.react:
			return

		# Spawn and run a new selector instance
		# stock_ids is an iterable (often a pymongo cursor)
		supplier = self.context.loader.new_context_unit(
			model = self.react.supply,
			context = self.context,
			sub_type = AbsT3Supplier,
			logger = logger,
			event_hdlr = event_hdlr,
			raise_exc = self.raise_exc,
			process_name = self.process_name,
			#_provenance = False
		)

		# Stager unit
		#############

		stager = self.context.loader.new_context_unit(
			model = self.react.stage,
			context = self.context,
			sub_type = AbsT3Stager,
			logger = logger,
			stock_updr = stock_updr,
			event_hdlr = event_hdlr,
			channel = (
				self.react.stage.config['channel'] # type: ignore
				if self.react.stage.config and self.react.stage.config.get('channel') # type: ignore[union-attr]
				else self.channel
			),
			raise_exc = self.raise_exc,
			process_name = self.process_name,
			#_provenance = False
		)

		if (t3_docs_gen := stager.stage(supplier.supply(), t3s)) is not None:

			for t3d in t3_docs_gen:

				if t3d is None:
					continue

				if 'meta' not in t3d:
					raise ValueError("Invalid T3Document, please check the associated stager unit")

				# Note: null = no provenance, 0 = args not serializable
				t3d['meta']['traceid'] = {'t3processor': self._trace_id}

				self.context.db.get_collection('t3').insert_one(t3d)
				if getattr(stager, 'propagate'):
					t3s.add_view(
						T3DocView.of(t3d, self.context.config)
					)


	@classmethod
	def spawn_logical_unit(cls,
		um: UnitModel,
		unit_type: Type[LT],
		loader: UnitLoader,
		logger: AmpelLogger,
		chan: Optional[ChannelId] = None
	) -> LT:
		""" Returns T3 unit instance """

		if logger.verbose:
			logger.log(VERBOSE, f"Instantiating unit {um.unit}")

		buf_hdlr = ChanRecordBufHandler(logger.level, chan, {'unit': um.unit}) if chan \
			else DefaultRecordBufferingHandler(logger.level, {'unit': um.unit})

		# Spawn unit instance
		t3_unit = loader.new_logical_unit(
			model = um,
			logger = AmpelLogger.get_logger(
				base_flag = (getattr(logger, 'base_flag', 0) & ~LogFlag.CORE) | LogFlag.UNIT,
				console = len(logger.handlers) == 1, # to be improved later
				handlers = [buf_hdlr]
			),
			sub_type = unit_type
		)

		setattr(t3_unit, '_buf_hdlr', buf_hdlr) # Shortcut
		return t3_unit
