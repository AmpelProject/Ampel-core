#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/demo/DemoProcessor.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 14.03.2021
# Last Modified Date: 19.03.2021
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from ampel.abstract.AbsProcessorUnit import AbsProcessorUnit
from ampel.model.UnitModel import UnitModel
from ampel.log import AmpelLogger
from ampel.abstract.AbsT3Unit import AbsT3Unit


class DemoProcessor(AbsProcessorUnit):

	parameter_a: int
	parameter_b: int = 200

	def run(self) -> None:

		from random import randint
		import time
		self.parameter_b = randint(3, 10)
		time.sleep(self.parameter_b)

		# Processor/Admin unit must instantiate their own loggger
		logger = AmpelLogger.get_logger()

		# Feedback
		logger.info("Method run() is being executed")

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
		unit = loader.new_base_unit(
			unit_model = UnitModel(unit = "DemoT3Unit"),
			logger = logger,
			sub_type = AbsT3Unit
		)

		# With which base units can be instantiated
		unit.done()
