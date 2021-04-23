#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/demo/DemoT3Unit.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 09.06.2020
# Last Modified Date: 22.04.2021
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Union, Generator, Tuple
from ampel.type.composed import T3Result
from ampel.view.SnapView import SnapView
from ampel.abstract.AbsT3Unit import AbsT3Unit
from ampel.struct.JournalTweak import JournalTweak
from ampel.struct.DocAttributes import DocAttributes


class DemoT3Unit(AbsT3Unit):

	def process(self, gen: Generator[SnapView, JournalTweak, None]) -> Union[
		None, T3Result, JournalTweak, Tuple[T3Result, JournalTweak]
	]:

		self.logger.info("DemoT3Unit output:")
		for v in gen:
			self.logger.info("id: " + str(v.id))
			gen.send(
				# This journal customization will be applied only to the current 'stock'
				JournalTweak(tag="DemoT3SpecificTag", extra={'a': 1})
			)

		# This journal customization will be applied to all stocks
		# return {'param': 'value'}, JournalTweak(tag="DemoT3UnitTag")
		return DocAttributes(data={'param': 'value'}, doc_code = 10), JournalTweak(tag="DemoT3UnitTag")
