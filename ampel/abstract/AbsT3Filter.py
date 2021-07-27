#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/t3/stage/filter/AbsT3Filter.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 12.02.2020
# Last Modified Date: 21.04.2021
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Sequence, Iterable
from ampel.base.AmpelABC import AmpelABC
from ampel.base.decorator import abstractmethod
from ampel.base.AmpelBaseModel import AmpelBaseModel
from ampel.struct.AmpelBuffer import AmpelBuffer


class AbsT3Filter(AmpelBaseModel, AmpelABC, abstract=True):

	@abstractmethod
	def filter(self, abufs: Iterable[AmpelBuffer]) -> Sequence[AmpelBuffer]:
		...
