#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/dev/NoShaper.py
# License:             BSD-3-Clause
# Author:              valery brinnel <firstname.lastname@gmail.com>
# Date:                05.08.2021
# Last Modified Date:  05.08.2021
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

from typing import Any
from collections.abc import Iterable
from ampel.types import StockId
from ampel.abstract.AbsT0Unit import AbsT0Unit
from ampel.content.DataPoint import DataPoint
from ampel.log.AmpelLogger import AmpelLogger


class NoShaper(AbsT0Unit):

	# override
	logger: None | AmpelLogger # type: ignore[assignment]

	# Mandatory implementation
	def process(self, arg: Iterable[dict[str, Any]], stock: StockId) -> list[DataPoint]: # type: ignore[override]
		return arg # type: ignore
