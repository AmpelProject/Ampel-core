#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/t1/T1SimpleCombiner.py
# License:             BSD-3-Clause
# Author:              valery brinnel <firstname.lastname@gmail.com>
# Date:                01.01.2018
# Last Modified Date:  14.06.2021
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

from typing import Iterable
from ampel.content.DataPoint import DataPoint
from ampel.types import DataPointId
from ampel.abstract.AbsT1CombineUnit import AbsT1CombineUnit


class T1SimpleCombiner(AbsT1CombineUnit):

	def combine(self, datapoints: Iterable[DataPoint]) -> list[DataPointId]:
		"""
		:param datapoints: dict instances representing datapoints
		"""

		if self.channel:
			return [
				dp['id'] for dp in datapoints
				if not("excl" in dp and self.channel in dp['excl'])
			]

		return [dp['id'] for dp in datapoints]
