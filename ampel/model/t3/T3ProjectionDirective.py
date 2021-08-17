#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/model/t3/T3ProjectionDirective.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 02.03.2021
# Last Modified Date: 02.03.2021
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>


from typing import Optional, Sequence
from pydantic import validator
from ampel.model.UnitModel import UnitModel
from ampel.model.StrictModel import StrictModel


class T3ProjectionDirective(StrictModel):
	"""
	Internal model used for field 'directives' of T3UnitRunner
	"""

	#: AbsT3Filter sub unit to use for down-selection of ampel buffers
	filter: Optional[UnitModel]

	#: AbsT3Projector sub unit capable of discarding selected ampel buffer attributes/fields
	project: Optional[UnitModel]

	#: t3 units (AbsT3Unit) to execute
	execute: Sequence[UnitModel]

	@validator('execute', pre=True)
	def cast_to_sequence_if_need_be(cls, v):
		if isinstance(v, dict):
			return [v]
		return v
