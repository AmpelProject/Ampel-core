#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/model/aux/AuxAliasableModel.py
# License           : BSD-3-Clause
# Author            : jvs
# Date              : Unspecified
# Last Modified Date: Unspecified
# Last Modified By  : jvs

from typing import Any

from pydantic import model_validator

from ampel.base.AmpelBaseModel import AmpelBaseModel
from ampel.base.AuxUnitRegister import AuxUnitRegister
from ampel.model.UnitModel import UnitModel


class AuxAliasableModel(AmpelBaseModel):
	"""
	A model that can be initialized from the name of an aux unit
	"""

	@model_validator(mode="before")
	def resolve_alias(cls: type["AuxAliasableModel"], value: Any) -> dict[str, Any]:
		if isinstance(value, str):
			if value in AuxUnitRegister._defs:  # noqa: SLF001
				return AuxUnitRegister.new_unit(model=UnitModel(unit=value), sub_type=cls).model_dump()
			raise ValueError(f"{cls.__name__} '{value}' not registered")
		return value
