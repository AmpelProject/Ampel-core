#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/t3/T3Processor.py
# License:             BSD-3-Clause
# Author:              valery brinnel <firstname.lastname@gmail.com>
# Date:                26.02.2018
# Last Modified Date:  03.04.2023
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

from typing import Annotated, Any

from ampel.abstract.AbsEventUnit import AbsEventUnit
from ampel.abstract.AbsT3Stager import AbsT3Stager
from ampel.abstract.AbsT3Supplier import AbsT3Supplier
from ampel.core.EventHandler import EventHandler
from ampel.log import SHOUT, AmpelLogger, LogFlag
from ampel.model.t3.T3IncludeDirective import T3IncludeDirective
from ampel.model.UnitModel import UnitModel
from ampel.struct.T3Store import T3Store
from ampel.types import ChannelId


class T3Processor(AbsEventUnit):

	# Require single channel for now (super classes allow multi-channel)
	channel: None | ChannelId = None

	include: None | T3IncludeDirective

	#: Unit must be a subclass of AbsT3Supplier
	supply: Annotated[UnitModel, AbsT3Supplier]

	#: Unit must be a subclass of AbsT3Stager
	stage: Annotated[UnitModel, AbsT3Stager]


	def post_init(self):
		if self.supply.unit not in self.context.config._config['unit']:  # noqa: SLF001
			raise ValueError(f"Unknown supply unit: {self.supply.unit}")
		if self.stage.unit not in self.context.config._config['unit']:  # noqa: SLF001
			raise ValueError(f"Unknown stager unit: {self.stage.unit}")


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

			t3s = T3Store()

			if self.include:

				if self.include.docs:
					pass # later

				if (x := self.include.session):
					sdict: dict[str, Any] = {}
					for model in [x] if isinstance(x, UnitModel) else x:
						rd = self.context.loader.new_context_unit(
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

			supplier = self.context.loader.new_context_unit(
				model = self.supply,
				context = self.context,
				sub_type = AbsT3Supplier,
				logger = logger,
				event_hdlr = event_hdlr
			)

			# Stager unit
			#############

			stager = self.context.loader.new_context_unit(
				model = self.stage,
				context = self.context,
				sub_type = AbsT3Stager,
				logger = logger,
				event_hdlr = event_hdlr,
				channel = (
					config['channel']
					if isinstance((config := self.stage.config), dict) and 'channel' in config
					else self.channel
				)
			)

			logger.info("Running stager", extra={'unit': self.stage.unit})

			if (doc_gen := stager.stage(supplier.supply(t3s), t3s)):
				for t3d in doc_gen:
					if 'meta' not in t3d:
						raise ValueError("Invalid T3Document")
					t3d['meta']['traceid'] = {'t3processor': self._trace_id}
					if event_hdlr.job_sig:
						t3d['meta']['jobid'] = event_hdlr.job_sig
					self.context.db.get_collection('t3').insert_one(t3d)

			"""
			if t3s.resources:
				for v in t3s.resources.values():
					event_hdlr.add_resource(v, overwrite=self.allow_resource_override)

			if t3s.aliases:
				for k, v in t3s.aliases.items():
					event_hdlr.add_alias(k, v, overwrite=self.allow_alias_override)
			"""


		except Exception as e:
			event_hdlr.handle_error(e, logger)

		# Feedback
		logger.log(SHOUT, f'Done running {self.process_name}')
		logger.flush()
