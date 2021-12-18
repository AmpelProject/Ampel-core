#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/t3/T3PlainUnitExecutor.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 12.12.2021
# Last Modified Date: 17.12.2021
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from time import time
from typing import Optional, Annotated, Generator

from ampel.view.T3Store import T3Store
from ampel.abstract.AbsT3ControlUnit import AbsT3ControlUnit
from ampel.abstract.AbsT3PlainUnit import AbsT3PlainUnit
from ampel.t3.T3DocBuilder import T3DocBuilder
from ampel.content.T3Document import T3Document
from ampel.model.UnitModel import UnitModel


class T3PlainUnitExecutor(AbsT3ControlUnit, T3DocBuilder):

	target: Annotated[UnitModel, AbsT3PlainUnit]

	def process(self, t3s: T3Store) -> Optional[Generator[T3Document, None, None]]:

		t3_unit = self.context.loader.new_safe_logical_unit(
			UnitModel(unit=self.target.unit, config=self.target.config),
			unit_type = AbsT3PlainUnit,
			logger = self.logger,
			_chan = self.channel
		)

		self.logger.info("Running T3unit", extra={'unit': self.target.unit})
		ret = t3_unit.process(t3s)
		self.flush(t3_unit)
		if ret:
			if x := self.handle_t3_result(t3_unit, ret, t3s, None, time()):
				yield x
