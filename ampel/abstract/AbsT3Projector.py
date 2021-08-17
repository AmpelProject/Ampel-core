#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/t3/stage/project/AbsT3Projector.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 21.01.2020
# Last Modified Date: 21.04.2021
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Sequence, Iterable
from ampel.base.AmpelABC import AmpelABC
from ampel.base.decorator import abstractmethod
from ampel.base.AmpelBaseModel import AmpelBaseModel
from ampel.struct.AmpelBuffer import AmpelBuffer


class AbsT3Projector(AmpelABC, AmpelBaseModel, abstract=True):

	@abstractmethod
	def project(self, seq: Iterable[AmpelBuffer]) -> Sequence[AmpelBuffer]:
		...
