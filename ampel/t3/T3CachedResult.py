#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/t3/T3CachedResult.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 26.11.2021
# Last Modified Date: 27.11.2021
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Any
from ampel.types import UBson
from ampel.abstract.AbsT3Unit import AbsT3Unit


class T3CachedResult(AbsT3Unit):

	unit: str
	confid: int
	content: UBson

	def process(self, gen: Any) -> UBson:
		self.logger.info(f"Returning cached result for unit {self.unit} with conf id {self.confid}")
		return self.content
