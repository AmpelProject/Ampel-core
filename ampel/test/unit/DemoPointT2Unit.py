#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/test/unit/DemoPointT2Unit.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 25.03.2020
# Last Modified Date: 25.03.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from time import time
from ampel.t2.T2UnitResult import T2UnitResult
from ampel.content.DataPoint import DataPoint
from ampel.abstract.AbsPointT2Unit import AbsPointT2Unit


class DemoPointT2Unit(AbsPointT2Unit):

	def run(self, datapoint: DataPoint) -> T2UnitResult:
		return {'result': {"time": time()}}
