#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/t3/T3ReviewUnitExecutor.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 12.12.2021
# Last Modified Date: 14.12.2021
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Optional, Generator, Annotated
from ampel.types import Traceless
from ampel.view.T3Store import T3Store
from ampel.abstract.AbsT3ControlUnit import AbsT3ControlUnit
from ampel.t3.T3DocBuilder import T3DocBuilder
from ampel.content.T3Document import T3Document
from ampel.model.UnitModel import UnitModel
from ampel.abstract.AbsT3Supplier import AbsT3Supplier
from ampel.abstract.AbsT3Stager import AbsT3Stager
from ampel.log.AmpelLogger import AmpelLogger


class T3ReviewUnitExecutor(AbsT3ControlUnit, T3DocBuilder):

	logger: Traceless[AmpelLogger]

	#: Unit must be a subclass of AbsT3Supplier
	supply: Annotated[UnitModel, AbsT3Supplier]

	#: Unit must be a subclass of AbsT3Stager
	stage: Annotated[UnitModel, AbsT3Stager]


	def process(self, t3s: T3Store) -> Optional[Generator[T3Document, None, None]]:

		try:

			supplier = self.context.loader.new_context_unit(
				model = self.supply,
				context = self.context,
				sub_type = AbsT3Supplier,
				logger = self.logger,
				event_hdlr = self.event_hdlr
			)

			# Stager unit
			#############

			stager = self.context.loader.new_context_unit(
				model = self.stage,
				context = self.context,
				sub_type = AbsT3Stager,
				logger = self.logger,
				event_hdlr = self.event_hdlr,
				channel = (
					self.stage.config['channel'] # type: ignore
					if self.stage.config and self.stage.config.get('channel') # type: ignore[union-attr]
					else self.channel
				)
			)

			return stager.stage(supplier.supply(t3s), t3s)

		except Exception as e:
			self.event_hdlr.handle_error(e, self.logger)

		return None
