#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/dev/NoShaper.py
# License:             BSD-3-Clause
# Author:              valery brinnel <firstname.lastname@gmail.com>
# Date:                05.08.2021
# Last Modified Date:  05.08.2021
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

from collections.abc import Iterable
from typing import Any

from ampel.abstract.AbsT0Unit import AbsT0Unit
from ampel.content.DataPoint import DataPoint
from ampel.types import StockId


class NoShaper(AbsT0Unit):

	# Mandatory implementation
	def process(self, arg: Iterable[dict[str, Any]], stock: StockId) -> list[DataPoint]: # type: ignore[override]
		return arg # type: ignore[return-value]
