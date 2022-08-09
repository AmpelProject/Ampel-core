#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/t3/unit/T3LogAggregatedStocks.py
# License:             BSD-3-Clause
# Author:              valery brinnel <firstname.lastname@gmail.com>
# Date:                14.05.2022
# Last Modified Date:  17.07.2022
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

from ampel.view.T3Store import T3Store
from ampel.abstract.AbsT3PlainUnit import AbsT3PlainUnit


class T3LogAggregatedStocks(AbsT3PlainUnit):

	input_unit: str = "T3AggregatingStager"

	def process(self, t3s: None | T3Store = None):

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

		return None
