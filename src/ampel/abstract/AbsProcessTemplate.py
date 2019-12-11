#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/abstract/AbsProcessTemplate.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 27.10.2019
# Last Modified Date: 27.10.2019
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from logging import Logger
from typing import Dict, Any
from ampel.abstract.AmpelABC import abstractmethod
from ampel.abstract.AbsAmpelBaseModel import AbsAmpelBaseModel
from ampel.model.ChannelModel import ChannelModel


class AbsProcessTemplate(ChannelModel, AbsAmpelBaseModel, abstract=True):
	""" 
	"""

	@abstractmethod	
	def get_process(self, logger: Logger) -> Dict[str, Any]:
		""" """
