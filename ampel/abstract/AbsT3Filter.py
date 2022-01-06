#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/t3/stage/filter/AbsT3Filter.py
# License:             BSD-3-Clause
# Author:              valery brinnel <firstname.lastname@gmail.com>
# Date:                12.02.2020
# Last Modified Date:  21.04.2021
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

from collections.abc import Iterable, Sequence
from ampel.base.AmpelABC import AmpelABC
from ampel.base.decorator import abstractmethod
from ampel.base.AmpelUnit import AmpelUnit
from ampel.struct.AmpelBuffer import AmpelBuffer


class AbsT3Filter(AmpelUnit, AmpelABC, abstract=True):

	@abstractmethod
	def filter(self, abufs: Iterable[AmpelBuffer]) -> Sequence[AmpelBuffer]:
		...
