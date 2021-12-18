#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/t3/T3Processor.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 26.02.2018
# Last Modified Date: 17.12.2021
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from importlib import import_module
from typing import Any, Optional, Annotated

from ampel.types import OneOrMany
from ampel.abstract.AbsEventUnit import AbsEventUnit
from ampel.abstract.AbsT3ControlUnit import AbsT3ControlUnit
from ampel.abstract.AbsProcessorTemplate import AbsProcessorTemplate
from ampel.view.T3Store import T3Store
from ampel.view.T3DocView import T3DocView
from ampel.model.UnitModel import UnitModel
from ampel.model.t3.T3IncludeDirective import T3IncludeDirective
from ampel.core.EventHandler import EventHandler
from ampel.abstract.AbsT3Supplier import AbsT3Supplier
from ampel.log import AmpelLogger, LogFlag, SHOUT


class T3Processor(AbsEventUnit):
	"""
	:param update_events: Record this event in the events collection
	:param extra_journal_tag:
	  tag(s) to add to the :class:`~ampel.content.JournalRecord.JournalRecord`
	  of each selected stock

	.. note:: by default, :func:`run` add entries to the journals of selected
	  stocks, store log records in the database, and records the invocation in
	  the events collection. To run an "anonymous" event that does not change
	  the state of the database:
	
	  - use a log_profile without db logging (profiles are defined under 'logging' in the ampel config)
	  - set update_events = False (no update to the events collection)
	  - set raise_exc = True (troubles collection will not be populated if an exception occurs)
	"""

	template: Optional[str] = None
	include: Optional[T3IncludeDirective]
	execute: OneOrMany[Annotated[UnitModel, AbsT3ControlUnit]]


	def __init__(self, **kwargs) -> None:

		if 'template' in kwargs:
			tpl_name = kwargs.pop("template")
			ctx = kwargs.pop("context")
			if ctx is None:
				raise ValueError("Context required")

			if kwargs['template'] not in ctx.config._config['template']:
				raise ValueError(f"Unknown process template: {tpl_name}")

			fqn = ctx.config._config['template'][tpl_name]
			class_name = fqn.split(".")[-1]
			Tpl = getattr(import_module(fqn), class_name)
			if not issubclass(Tpl, AbsProcessorTemplate):
				raise ValueError(f"Unexpected template type: {Tpl}")

			tpl = Tpl(**kwargs)
			kwargs = tpl.get_model(ctx.config._config, kwargs).dict()
			kwargs['context'] = ctx

		super().__init__(**kwargs)
		self.update_events = True


	def set_no_event(self) -> None:
		""" run t3 process \"silently\" """
		self.update_events = False


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
					for model in [x] if isinstance(x, UnitModel) else x:
						rd = loader.new_context_unit(
							model = model,
							context = self.context,
							sub_type = AbsT3Supplier,
							event_hdlr = event_hdlr,
							logger = logger
						).supply(t3s)
						if rd:
							sdict |= rd
					if sdict:
						t3s.add_session_info(sdict)

			units = self.context.config._config['unit']
			for i, um in enumerate([self.execute] if isinstance(self.execute, UnitModel) else self.execute):

				if um.unit not in units:
					raise ValueError(f"Unknown unit: {um.unit}")

				if "AbsT3ControlUnit" not in units[um.unit]['base']:
					raise ValueError("T3Processor executes only AbsT3ControlUnit units")

				t3_unit = loader.new_context_unit(
					model = um,
					context = self.context,
					sub_type = AbsT3ControlUnit,
					logger = logger,
					event_hdlr = event_hdlr,
					channel = self.channel
				)

				logger.info(
					f"Processing run block {i}",
					extra={'unit': t3_unit.__class__.__name__}
				)

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
