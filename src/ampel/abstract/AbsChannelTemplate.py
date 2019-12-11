#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/abstract/AbsChannelTemplate.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 27.10.2019
# Last Modified Date: 27.10.2019
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from logging import Logger
from typing import Dict, Any, Optional
from ampel.model.ChannelModel import ChannelModel
from ampel.abstract.AmpelABC import abstractmethod
from ampel.abstract.AbsAmpelBaseModel import AbsAmpelBaseModel


class AbsChannelTemplate(AbsAmpelBaseModel, ChannelModel, abstract=True):
	""" 
	"""
	template: Optional[str]

	@abstractmethod	
	def get_channel(self, logger: Logger) -> Dict[str, Any]:
		""" """

	@abstractmethod	
	def get_processes(self, logger: Logger) -> Dict[str, Any]:
		""" """
