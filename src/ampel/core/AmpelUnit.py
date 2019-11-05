#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/core/AmpelUnit.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 04.10.2019
# Last Modified Date: 11.10.2019
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from pydantic import BaseModel
from typing import Dict, Type, Any, Union
from ampel.logging.AmpelLogger import AmpelLogger
from ampel.model.BetterConfigDefaults import BetterConfigDefaults
from ampel.abstract.AbsAmpelUnit import AbsAmpelUnit


class AmpelUnit(BaseModel):
	"""
	Holds information related to user contributed units 
	(Filters, T2s, T3s), configuration and resources
	"""

	Config = BetterConfigDefaults

	unit_class: Type[AbsAmpelUnit]
	init_config: Union[None, BaseModel, Dict[str, Any]]
	resources: Dict[str, Any]

	def instantiate(self, logger: AmpelLogger) -> AbsAmpelUnit:
		return self.unit_class(
			logger, 
			init_config=self.init_config, 
			resources=self.resources
		)

	def get_init_config(self) -> Union[None, BaseModel, Dict[str, Any]]:
		return self.init_config


	def get_resources(self) -> Dict[str, Any]:
		return self.resources
