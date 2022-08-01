#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/demo/DemoProcessor.py
# License:             BSD-3-Clause
# Author:              valery brinnel <firstname.lastname@gmail.com>
# Date:                14.03.2021
# Last Modified Date:  01.08.2022
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

from typing import Any
from ampel.abstract.AbsEventUnit import AbsEventUnit
from ampel.model.UnitModel import UnitModel
from ampel.log import AmpelLogger
from ampel.core.EventHandler import EventHandler
from ampel.abstract.AbsT3ReviewUnit import AbsT3ReviewUnit


class DemoProcessor(AbsEventUnit):

	parameter_a: int
	parameter_b: int = 200

	def proceed(self, event_hdlr: EventHandler) -> Any:

		from random import randint
		import time
		self.parameter_b = randint(3, 10)
		time.sleep(self.parameter_b)

		# Processor/Admin unit must instantiate their own loggger
		logger = AmpelLogger.get_logger()

		# Feedback
		logger.info("Executing run()")

		# Processor units have access to the associated process id
		logger.info(f"Requested by process {self.process_name}")

		# Config parameters
		logger.info(f"Mandatory config parameter 'parameter_a' value: {self.parameter_a}")
		logger.info(f"Optional config parameter 'parameter_b' value: {self.parameter_b}")


		# Process/Admin units have access to AmpelContext
		ctx = self.context

		# Which contains an instance of UnitLoader
		loader = ctx.loader

		# With which base units can be instantiated
		unit = loader.new_logical_unit(
			model = UnitModel(unit = "DemoReviewT3Unit"),
			logger = logger,
			sub_type = AbsT3ReviewUnit
		)

		print(unit)
