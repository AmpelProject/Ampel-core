#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/t3/stage/project/AbsT3Projector.py
# License:             BSD-3-Clause
# Author:              valery brinnel <firstname.lastname@gmail.com>
# Date:                21.01.2020
# Last Modified Date:  21.04.2021
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

from typing import Sequence
from collections.abc import Iterable
from ampel.base.AmpelABC import AmpelABC
from ampel.base.decorator import abstractmethod
from ampel.base.AmpelUnit import AmpelUnit
from ampel.struct.AmpelBuffer import AmpelBuffer


class AbsT3Projector(AmpelABC, AmpelUnit, abstract=True):

	@abstractmethod
	def project(self, seq: Iterable[AmpelBuffer]) -> Sequence[AmpelBuffer]:
		...
