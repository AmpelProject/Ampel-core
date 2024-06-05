#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/t4/T4Processor.py
# License:             BSD-3-Clause
# Author:              valery brinnel <firstname.lastname@gmail.com>
# Date:                02.04.2023
# Last Modified Date:  04.04.2023
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

from collections.abc import Sequence
from datetime import datetime, timezone
from typing import Annotated

from ampel.abstract.AbsEventUnit import AbsEventUnit
from ampel.abstract.AbsT4ControlUnit import AbsT4ControlUnit
from ampel.abstract.AbsT4Unit import AbsT4Unit
from ampel.content.T4Document import T4Document
from ampel.core.DocBuilder import DocBuilder
from ampel.core.EventHandler import EventHandler
from ampel.log import SHOUT, AmpelLogger, LogFlag
from ampel.model.UnitModel import UnitModel
from ampel.types import ChannelId


class T4Processor(AbsEventUnit, DocBuilder):

	execute: Annotated[Sequence[UnitModel], AbsT4Unit | AbsT4ControlUnit]
	channel: None | ChannelId = None

	def __init__(self, **kwargs) -> None:
		if isinstance(kwargs.get('execute', []), dict):
			kwargs['execute'] = [kwargs['execute']]
		super().__init__(**kwargs)


	def proceed(self, event_hdlr: EventHandler) -> None:

		event_hdlr.set_tier(4)
		logger = AmpelLogger.from_profile(
			self.context, self.log_profile, event_hdlr.get_run_id(),
			base_flag = LogFlag.T4 | LogFlag.CORE | self.base_log_flag,
			force_refresh = True
		)

		try:

			for um in self.execute:

				t4_unit_info = self.context.config.get(f'unit.{um.unit}', dict, raise_exc=True)
				if 'AbsT4Unit' in t4_unit_info['base']:
					t4_unit: AbsT4Unit | AbsT4ControlUnit = self.context.loader.new_safe_logical_unit(
						um=um, unit_type=AbsT4Unit, logger=logger,
						_chan=self.channel # unclear if multiple chans are supported
					)

				else:
					t4_unit = self.context.loader.new_context_unit(
						model = um,
						context = self.context,
						sub_type = AbsT4ControlUnit,
						logger = logger,
						event_hdlr = event_hdlr,
						channel = self.channel
					)

				logger.log(SHOUT, f'Running {self.process_name}', extra={'unit': t4_unit.__class__.__name__})

				ts = datetime.now(tz=timezone.utc).timestamp()

				if isinstance(t4_unit, AbsT4Unit):

					ret1 = t4_unit.do()
					if (buf_hdlr := getattr(t4_unit, '_buf_hdlr', None)) and buf_hdlr.buffer:
						buf_hdlr.forward(logger)

					if not ret1:
						continue

					t4ds: Sequence[T4Document] = [
						self.craft_doc(event_hdlr, t4_unit, ret1, ts, doc_type=T4Document)
					]

				elif isinstance(t4_unit, AbsT4ControlUnit):
					t4ds = list(t4_unit.do())

				# T4Document to be included in t4 collection
				for t4d in t4ds:

					if 'meta' not in t4d:
						raise ValueError("Invalid T4Document")

					t4d['meta']['traceid'] = {'t4processor': self._trace_id}
					if event_hdlr.job_sig:
						t4d['meta']['jobid'] = event_hdlr.job_sig

					self.context.db.get_collection('t4').insert_one(t4d)

		except Exception as e:
			event_hdlr.handle_error(e, logger)

		# Feedback
		logger.log(SHOUT, f'Done running {self.process_name}')
		logger.flush()
