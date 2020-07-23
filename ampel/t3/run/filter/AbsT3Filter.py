#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/t3/run/filter/AbsT3Filter.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 12.02.2020
# Last Modified Date: 20.06.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Sequence
from ampel.base import AmpelABC, AmpelBaseModel, abstractmethod
from ampel.core.AmpelBuffer import AmpelBuffer


class AbsT3Filter(AmpelBaseModel, AmpelABC, abstract=True):

	@abstractmethod
	def filter(self, abufs: Sequence[AmpelBuffer]) -> Sequence[AmpelBuffer]:
		...
