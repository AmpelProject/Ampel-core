#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/abstract/AbsApplicable.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 27.02.2020
# Last Modified Date: 27.02.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Optional, Any
from ampel.abc import abstractmethod
from ampel.abstract.AbsAuxiliaryUnit import AbsAuxiliaryUnit

class AbsApplicable(AbsAuxiliaryUnit, abstract=True):
	""" """

	@abstractmethod
	def apply(self, arg: Any) -> Optional[Any]:
		...
