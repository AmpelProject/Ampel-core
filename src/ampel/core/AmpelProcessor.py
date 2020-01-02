#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/abstract/AbsAmpelProcessor.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 07.10.2019
# Last Modified Date: 22.10.2019
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from pydantic import BaseModel
from typing import Dict, Any, Union
from ampel.config.AmpelBaseConfig import AmpelBaseConfig
from ampel.model.AmpelBaseModel import AmpelBaseModel
from ampel.abstract.AmpelABC import AmpelABC, abstractmethod

class AbsAmpelProcessor(metaclass=AmpelABC):
	"""
	"""

	@abstractmethod
	def __init__(
		self, ampel_config: AmpelBaseConfig, 
		init_config: Union[AmpelBaseModel, BaseModel, Dict[str, Any]]
	):
		""" """

	@abstractmethod
	def run(self):
		""" """
