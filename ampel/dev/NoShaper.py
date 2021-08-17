#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/dev/NoShaper.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 05.08.2021
# Last Modified Date: 05.08.2021
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Dict, List, Any, Iterable, Optional
from ampel.types import StockId
from ampel.abstract.AbsT0Unit import AbsT0Unit
from ampel.content.DataPoint import DataPoint
from ampel.log.AmpelLogger import AmpelLogger


class NoShaper(AbsT0Unit):

	# override
	logger: Optional[AmpelLogger] # type: ignore[assignment]

	# Mandatory implementation
	def process(self, arg: Iterable[Dict[str, Any]], stock: StockId) -> List[DataPoint]: # type: ignore[override]
		return arg # type: ignore
