#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/model/t3/T3Directive.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 04.06.2020
# Last Modified Date: 04.06.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Optional, Sequence
from ampel.model.UnitModel import UnitModel
from ampel.model.StrictModel import StrictModel

class T3Directive(StrictModel):
	"""
	Specification of a Tier 3 processing sequence.
	"""

	#: Provide context for run
	context: Optional[Sequence[UnitModel]]
	#: Select stocks
	select: Optional[UnitModel]
	#: Fill :class:`~ampel.core.AmpelBuffer.AmpelBuffer` for each selected stock
	load: Optional[UnitModel]
	#: Add external information to each :class:`~ampel.core.AmpelBuffer.AmpelBuffer`.
	complement: Optional[Sequence[UnitModel]]
	#: Execute units on each view
	run: UnitModel
