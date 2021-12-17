#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/demo/DemoPlainT3Unit.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 17.12.2021
# Last Modified Date: 17.12.2021
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Union
from ampel.types import UBson
from ampel.struct.UnitResult import UnitResult
from ampel.view.T3Store import T3Store
from ampel.abstract.AbsT3PlainUnit import AbsT3PlainUnit


class DemoPlainT3Unit(AbsT3PlainUnit):

	a_parameter: int = 9000
	my_t3_doc_tag: str = "A_TAG"


	def post_init(self) -> None:
		self.logger.info("post_init was called")


	def process(self, t3s: T3Store) -> Union[UBson, UnitResult]:

		self.logger.info("Running DemoPlainT3Unit")

		if not t3s.units:
			self.logger.info(f"T3 store contains t3 views for units: {t3s.units}")
		else:
			self.logger.info("T3 store contains no t3 views")

		return UnitResult(
			body = {'a_parameter': self.a_parameter},
			tag = self.my_t3_doc_tag
		)
