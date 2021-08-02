#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/abstract/AbsSessionInfo.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 06.01.2020
# Last Modified Date: 03.05.2021
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Dict, Any
from ampel.core.ContextUnit import ContextUnit
from ampel.base.AmpelABC import AmpelABC
from ampel.base.decorator import abstractmethod
from ampel.log.AmpelLogger import AmpelLogger


class AbsSessionInfo(AmpelABC, ContextUnit, abstract=True):
	"""
	Add contextual information to a T3 process, such as the time of the previous
	run, or the number of alerts processed since that run.
	
	Inherits from ContextUnit because subclasses might need access to
	the AmpelConfig (foremost to the contained resource definitions)
	"""

	logger: AmpelLogger
	process_name: str

	@abstractmethod
	def update(self, session_info: Dict[str, Any]) -> None:
		"""
		:param run_context: run context to fill
		"""
