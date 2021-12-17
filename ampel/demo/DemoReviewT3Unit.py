#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/demo/DemoReviewT3Unit.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 09.06.2020
# Last Modified Date: 17.12.2021
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Union, Generator
from ampel.types import UBson, T3Send
from ampel.abstract.AbsT3ReviewUnit import AbsT3ReviewUnit
from ampel.struct.JournalAttributes import JournalAttributes
from ampel.struct.UnitResult import UnitResult
from ampel.view.SnapView import SnapView
from ampel.view.T3Store import T3Store


class DemoReviewT3Unit(AbsT3ReviewUnit[SnapView]):

	parameter: int = 10

	def process(self,
		gen: Generator[SnapView, T3Send, None],
		t3s: T3Store
	) -> Union[UBson, UnitResult]:

		self.logger.info(f"DemoReviewT3Unit output (parameter={self.parameter}):")

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
			journal = JournalAttributes(tag="DemoReviewT3UnitTag")
		)
