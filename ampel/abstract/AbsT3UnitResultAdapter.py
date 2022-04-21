#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/abstract/AbsT3UnitResultAdapter.py
# License:             BSD-3-Clause
# Author:              valery brinnel <firstname.lastname@gmail.com>
# Date:                20.04.2022
# Last Modified Date:  20.04.2022
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

from ampel.view.T3Store import T3Store
from ampel.base.AmpelABC import AmpelABC
from ampel.base.decorator import abstractmethod
from ampel.struct.UnitResult import UnitResult
from ampel.core.ContextUnit import ContextUnit


class AbsT3UnitResultAdapter(ContextUnit, AmpelABC, abstract=True):

	@abstractmethod
	def handle(self, ur: UnitResult, t3s: T3Store) -> UnitResult:
		...
