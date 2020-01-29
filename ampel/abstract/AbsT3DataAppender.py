#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/abstract/AbsT3DataAppender.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 16.01.2020
# Last Modified Date: 29.01.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Sequence
from ampel.t3.SnapData import SnapData
from ampel.abstract.AmpelABC import abstractmethod
from ampel.abstract.AmpelProcessor import AmpelProcessor
from ampel.model.AmpelBaseModel import AmpelBaseModel


class AbsT3DataAppender(AmpelProcessor, abstract=True):
	"""
	Subclass of AmpelProcessor because subclasses might need access to
	the AmpelConfig (foremost to the contained resource definitions)
	"""

	class InitOptions(AmpelBaseModel):
		""" """
		verbose: bool = False
		debug: bool = False


	@abstractmethod
	def update(self, snap_data_seq: Sequence[SnapData]) -> None:
		""" """
