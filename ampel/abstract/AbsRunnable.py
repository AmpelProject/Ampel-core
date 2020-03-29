#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/abstract/AbsRunnable.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 04.02.2020
# Last Modified Date: 04.02.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from ampel.abc import abstractmethod
from ampel.abstract.AbsProcessorUnit import AbsProcessorUnit

class AbsRunnable(AbsProcessorUnit, abstract=True):

	@abstractmethod
	def run(self) -> None:
		...
