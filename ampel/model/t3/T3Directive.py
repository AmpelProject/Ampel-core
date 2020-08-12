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

	context: Optional[Sequence[UnitModel]]
	select: Optional[UnitModel]
	load: Optional[UnitModel]
	complement: Optional[Sequence[UnitModel]]
	run: UnitModel
