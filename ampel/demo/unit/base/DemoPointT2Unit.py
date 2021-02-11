#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/demo/unit/base/DemoPointT2Unit.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 25.03.2020
# Last Modified Date: 14.06.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from time import time
from ampel.type import T2UnitResult
from ampel.content.DataPoint import DataPoint
from ampel.abstract.AbsPointT2Unit import AbsPointT2Unit


class DemoPointT2Unit(AbsPointT2Unit):

	test_parameter: int = 1

	def run(self, datapoint: DataPoint) -> T2UnitResult:
		return {
			"datapoint_id": datapoint['_id'],
			"time": time(),
			"test_parameter": self.test_parameter
		}
