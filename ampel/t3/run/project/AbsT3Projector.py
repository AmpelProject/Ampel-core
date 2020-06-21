#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/t3/run/project/AbsT3Projector.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 21.01.2020
# Last Modified Date: 18.06.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Sequence
from ampel.base import AmpelBaseModel, AmpelABC, abstractmethod
from ampel.core.AmpelBuffer import AmpelBuffer


class AbsT3Projector(AmpelABC, AmpelBaseModel, abstract=True):


	@abstractmethod
	def project(self, seq: Sequence[AmpelBuffer]) -> Sequence[AmpelBuffer]:
		pass
