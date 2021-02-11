#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/demo/unit/base/DemoT3Unit.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 09.06.2020
# Last Modified Date: 14.06.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Tuple
from ampel.type import T3AddResult
from ampel.view.SnapView import SnapView
from ampel.abstract.AbsT3Unit import AbsT3Unit
from ampel.struct.JournalTweak import JournalTweak


class DemoT3Unit(AbsT3Unit):


	def add(self, views: Tuple[SnapView, ...]) -> T3AddResult:
		for v in views:
			print(v.serialize())
			print("-" * 30)
		return JournalTweak(tag="DemoTag")


	def done(self) -> None:
		print("done")
