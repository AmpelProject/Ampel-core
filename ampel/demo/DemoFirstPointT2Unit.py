#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/demo/DemoFirstPointT2Unit.py
# License:             BSD-3-Clause
# Author:              valery brinnel <firstname.lastname@gmail.com>
# Date:                25.03.2020
# Last Modified Date:  30.05.2021
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

from time import time
from ampel.types import UBson
from ampel.struct.UnitResult import UnitResult
from ampel.content.DataPoint import DataPoint
from ampel.abstract.AbsPointT2Unit import AbsPointT2Unit


class DemoFirstPointT2Unit(AbsPointT2Unit):

	ingest = {'filter': 'PPSFilter', 'sort': 'jd', 'select': 'first'}

	def process(self, datapoint: DataPoint) -> UBson | UnitResult:
		return {"time": time()}
