#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/abstract/AbsT3ContextAppender.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 06.01.2020
# Last Modified Date: 29.01.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Dict, Any
from ampel.abstract.AmpelABC import abstractmethod
from ampel.abstract.AmpelProcessor import AmpelProcessor


class AbsT3ContextAppender(AmpelProcessor, abstract=True): # type: ignore
	"""
	Context definition: "the circumstances that form the setting for an event"
	Subclass of AmpelProcessor because subclasses might need access to
	the AmpelConfig (foremost to the contained resource definitions)
	"""

	@abstractmethod
	def update(self, context: Dict[str, Any]) -> None:
		""" Appends key, values to the provided context dict instance """
