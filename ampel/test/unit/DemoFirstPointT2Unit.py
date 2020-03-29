#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/test/unit/DemoFirstPointT2Unit.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 25.03.2020
# Last Modified Date: 25.03.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from time import time
from typing import Dict
from ampel.t2.T2Result import T2Result
from ampel.content.DataPoint import DataPoint
from ampel.abstract.AbsPointT2Unit import AbsPointT2Unit


class DemoFirstPointT2Unit(AbsPointT2Unit):

	ingest: Dict = {'eligible': 'first'}

	def run(self, datapoint: DataPoint) -> T2Result:
		return {'result': {"time": time()}}
