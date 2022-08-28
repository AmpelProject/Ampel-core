#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/abstract/AbsUnitResultAdapter.py
# License:             BSD-3-Clause
# Author:              valery brinnel <firstname.lastname@gmail.com>
# Date:                20.04.2022
# Last Modified Date:  28.08.2022
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

from ampel.base.AmpelABC import AmpelABC
from ampel.base.decorator import abstractmethod
from ampel.struct.UnitResult import UnitResult
from ampel.core.ContextUnit import ContextUnit


class AbsUnitResultAdapter(ContextUnit, AmpelABC, abstract=True):

	run_id: int

	@abstractmethod
	def handle(self, ur: UnitResult) -> UnitResult:
		...
