#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/demo/DemoT3Unit.py
# License:             BSD-3-Clause
# Author:              valery brinnel <firstname.lastname@gmail.com>
# Date:                09.06.2020
# Last Modified Date:  03.04.2023
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

from collections.abc import Generator

from ampel.abstract.AbsT3Unit import AbsT3Unit
from ampel.struct.JournalAttributes import JournalAttributes
from ampel.struct.T3Store import T3Store
from ampel.struct.UnitResult import UnitResult
from ampel.types import T3Send, UBson
from ampel.view.SnapView import SnapView


class DemoT3Unit(AbsT3Unit[SnapView]):

	parameter: int = 10

	def process(self,
		gen: Generator[SnapView, T3Send, None],
		t3s: T3Store
	) -> UBson | UnitResult:

		self.logger.info(f"DemoT3Unit output (parameter={self.parameter}):")

		for v in gen:

			self.logger.info("id: " + str(v.id))
			gen.send(
				# This journal customization will be applied only to the current 'stock'
				JournalAttributes(tag="DemoT3SpecificTag", extra={'a': 1})
			)

		# This journal customization will be applied to all stocks
		return UnitResult(
			body = {'param': 'value'},
			code = 10,
			tag = "T3DocTag",
			journal = JournalAttributes(tag="DemoT3UnitTag")
		)
