#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/t3/T3Processor.py
# License:             BSD-3-Clause
# Author:              valery brinnel <firstname.lastname@gmail.com>
# Date:                26.02.2018
# Last Modified Date:  25.07.2022
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

from importlib import import_module
from typing import Any, Annotated

from ampel.types import OneOrMany
from ampel.abstract.AbsEventUnit import AbsEventUnit
from ampel.abstract.AbsT3Supplier import AbsT3Supplier
from ampel.abstract.AbsT3ControlUnit import AbsT3ControlUnit
from ampel.abstract.AbsProcessorTemplate import AbsProcessorTemplate
from ampel.view.T3Store import T3Store
from ampel.view.T3DocView import T3DocView
from ampel.model.UnitModel import UnitModel
from ampel.model.t3.T3IncludeDirective import T3IncludeDirective
from ampel.core.EventHandler import EventHandler
from ampel.log import AmpelLogger, LogFlag, SHOUT


class T3Processor(AbsEventUnit):
	""" """

	template: None | str = None
	include: None | T3IncludeDirective
	execute: Annotated[OneOrMany[UnitModel], AbsT3ControlUnit]


	def __init__(self, **kwargs) -> None:

		if 'template' in kwargs:
			tpl_name = kwargs.pop("template")
			ctx = kwargs.pop("context")
			if ctx is None:
				raise ValueError("Context required")

			if tpl_name not in ctx.config._config.get('template', []):
				raise ValueError(f"Unknown process template: {tpl_name}")

			fqn = ctx.config._config['template'][tpl_name]
			class_name = fqn.split(".")[-1]
			Tpl = getattr(import_module(fqn), class_name)
			if not issubclass(Tpl, AbsProcessorTemplate):
				raise ValueError(f"Unexpected template type: {Tpl}")

			tpl = Tpl(
				**{
					k: v for k, v in kwargs.items()
					if k not in AbsEventUnit._annots
				}
			)
			kwargs.update(
				tpl.get_model(ctx.config._config, kwargs).dict()['config']
			)
			kwargs['context'] = ctx

		super().__init__(**kwargs)


	def proceed(self, event_hdlr: EventHandler) -> None:

		event_hdlr.set_tier(3)
		logger = AmpelLogger.from_profile(
			self.context, self.log_profile, event_hdlr.get_run_id(),
			base_flag = LogFlag.T3 | LogFlag.CORE | self.base_log_flag,
			force_refresh = True
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
						if event_hdlr.job_sig:
							t3d['meta']['jobid'] = event_hdlr.job_sig
						self.context.db.get_collection('t3').insert_one(t3d) # type: ignore[arg-type]


		except Exception as e:
			event_hdlr.handle_error(e, logger)

		# Feedback
		logger.log(SHOUT, f'Done running {self.process_name}')
		logger.flush()
