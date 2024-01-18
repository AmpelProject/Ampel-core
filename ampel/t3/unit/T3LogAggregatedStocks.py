#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/t3/unit/T3LogAggregatedStocks.py
# License:             BSD-3-Clause
# Author:              valery brinnel <firstname.lastname@gmail.com>
# Date:                14.05.2022
# Last Modified Date:  03.04.2023
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

from typing import Any

from ampel.abstract.AbsT3Unit import AbsT3Unit
from ampel.struct.T3Store import T3Store


class T3LogAggregatedStocks(AbsT3Unit):

	input_unit: str = "T3AggregatingStager"

	def process(self, gen: Any, t3s: T3Store):

		self.logger.info(f"Running {self.__class__.__name__}")

		if not t3s:
			raise ValueError("A T3 store is required, please check your config")

		self.logger.info(
			"Stock ids",
			extra = {
				'stock': [
					int(k) for k in t3s \
						.get_mandatory_view(unit=self.input_unit) \
						.get_body(raise_exc=True)
				]
			}
		)

