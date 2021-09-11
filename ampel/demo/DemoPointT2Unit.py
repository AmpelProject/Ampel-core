#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/demo/DemoPointT2Unit.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 25.03.2020
# Last Modified Date: 11.09.2021
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from time import time
from typing import Union
from random import randint
from ampel.types import UBson
from ampel.struct.UnitResult import UnitResult
from ampel.content.DataPoint import DataPoint
from ampel.abstract.AbsPointT2Unit import AbsPointT2Unit


class DemoPointT2Unit(AbsPointT2Unit):

	test_parameter: int = 1

	def process(self, datapoint: DataPoint) -> Union[UBson, UnitResult]:

		ret = {
			"datapoint_id": datapoint['id'],
			"time": time(),
			"test_parameter": self.test_parameter
		}

		if randint(0, 1) > 0.5:
			return ret
		else:
			return UnitResult(code=10, tag="MY_CUSTOM_TAG", body=ret)
