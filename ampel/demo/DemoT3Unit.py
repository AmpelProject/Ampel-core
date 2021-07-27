#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/demo/DemoT3Unit.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 09.06.2020
# Last Modified Date: 17.06.2021
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Union, Generator
from ampel.types import UBson
from ampel.abstract.AbsT3Unit import AbsT3Unit
from ampel.struct.JournalAttributes import JournalAttributes
from ampel.struct.UnitResult import UnitResult
from ampel.view.SnapView import SnapView


class DemoT3Unit(AbsT3Unit[SnapView]):

	def process(self, gen: Generator[SnapView, JournalAttributes, None]) -> Union[UBson, UnitResult]:

		self.logger.info("DemoT3Unit output:")

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
			journal = JournalAttributes(tag="DemoT3UnitTag")
		)
