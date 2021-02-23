#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/t3/context/AbsT3RunContextAppender.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 06.01.2020
# Last Modified Date: 10.06.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Dict, Any
from ampel.base import abstractmethod
from ampel.log.AmpelLogger import AmpelLogger
from ampel.core.AdminUnit import AdminUnit


class AbsT3RunContextAppender(AdminUnit, abstract=True):
	"""
	Add contextual information to a T3 process, such as the time of the previous
	run, or the number of alerts processed since that run.
	
	Inherits from AdminUnit because subclasses might need access to
	the AmpelConfig (foremost to the contained resource definitions)
	"""

	logger: AmpelLogger
	process_name: str

	@abstractmethod
	def update(self, run_context: Dict[str, Any]) -> None:
		"""
		:param run_context: run context to fill
		"""
		...
