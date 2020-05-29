#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/model/AmpelStrictModel.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 30.09.2018
# Last Modified Date: 14.05.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from pydantic import BaseModel, BaseConfig, Extra
from ampel.config.AmpelConfig import AmpelConfig

class AmpelStrictModel(BaseModel):

	class Config(BaseConfig):
		arbitrary_types_allowed = True
		allow_population_by_field_name = True
		validate_all = True

	def __init__(self, **kwargs):
		""" Raises validation errors if extra fields are present """

		self.__config__.extra = Extra.forbid
		if AmpelConfig._check_types:
			BaseModel.__init__(self, **kwargs)
		else:
			BaseModel.construct(self, **kwargs)
		self.__config__.extra = Extra.allow
