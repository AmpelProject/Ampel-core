#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/model/aux/AuxAliasableModel.py
# License           : BSD-3-Clause
# Author            : jvs
# Date              : Unspecified
# Last Modified Date: Unspecified
# Last Modified By  : jvs

from typing import Any, Type
from ampel.base.AmpelBaseModel import AmpelBaseModel
from ampel.base.AuxUnitRegister import AuxUnitRegister


class AuxAliasableModel(AmpelBaseModel):
	"""
	A model that can be initialized from the name of an aux unit
	"""

	@classmethod
	def validate(cls: Type["AuxAliasableModel"], value: Any) -> "AuxAliasableModel":
		if isinstance(value, str):
			if value in AuxUnitRegister._defs:
				return AuxUnitRegister.get_aux_class(value).validate({})
			else:
				raise ValueError(f"{cls.__name__} '{value}' not registered")
		return super().validate(value)
