#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/abstract/AbsBufferComplement.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 16.01.2020
# Last Modified Date: 13.12.2021
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Iterable
from ampel.types import Traceless
from ampel.view.T3Store import T3Store
from ampel.protocol.LoggerProtocol import LoggerProtocol
from ampel.base.AmpelABC import AmpelABC
from ampel.base.decorator import abstractmethod
from ampel.struct.AmpelBuffer import AmpelBuffer
from ampel.core.ContextUnit import ContextUnit


# Inherits ContextUnit because implementing classes might need access to
# an AmpelConfig instance (foremost to the contained resource definitions)
class AbsBufferComplement(AmpelABC, ContextUnit, abstract=True):
	"""
	Complement :class:`~ampel.core.AmpelBuffer.AmpelBuffer` with information
	stored outside the Ampel database.
	"""

	logger: Traceless[LoggerProtocol]

	@abstractmethod
	def complement(self, it: Iterable[AmpelBuffer], t3s: T3Store) -> None:
		"""Add information to each :class:`~ampel.core.AmpelBuffer.AmpelBuffer` """
		...
