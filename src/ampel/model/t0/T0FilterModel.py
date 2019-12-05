#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/model/t0/T0FilterModel.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 03.11.2019
# Last Modified Date: 03.11.2019
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import List, Optional, Sequence
from ampel.model.AmpelBaseModel import AmpelBaseModel
from ampel.model.UnitModel import UnitModel

class T0FilterModel(AmpelBaseModel):
	""" """
	unit: UnitModel
	# Later
	# t1Combine: List[T1CombineData]
	t2_compute: List[UnitModel]
	on_match_t2_units: Optional[Sequence[str]] = ()
