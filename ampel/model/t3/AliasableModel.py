#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/model/t3/AliasableModel.py
# License:             BSD-3-Clause
# Author:              jvs
# Date:                Unspecified
# Last Modified Date:  Unspecified
# Last Modified By:    jvs

from typing import Any, ClassVar, TYPE_CHECKING
from ampel.base.AmpelBaseModel import AmpelBaseModel

if TYPE_CHECKING:
	from ampel.config.AmpelConfig import AmpelConfig


class AliasableModel(AmpelBaseModel):
	"""
	A model that can be initialized from a global alias in the alias.t3 section
	of an AmpelConfig
	"""

	_config: ClassVar['None | AmpelConfig'] = None

	@classmethod
	def validate(cls, value: Any) -> "AliasableModel":
		if cls._config and isinstance(value, str):
			d = cls._config.get(f"alias.t3.%{value}", dict)
			if d:
				value = d
			else:
				raise ValueError(f"{cls.__name__} alias '{value}' not found in Ampel config")
		return super().validate(value)
