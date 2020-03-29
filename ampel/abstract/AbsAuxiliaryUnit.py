#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/abstract/AbsAuxiliaryUnit.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 18.02.2020
# Last Modified Date: 19.03.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Dict, Any, ClassVar
from pydantic import BaseModel, Extra
from ampel.abc import defaultmethod
from ampel.abc.AmpelABC import AmpelABC


class AbsAuxiliaryUnit(BaseModel, AmpelABC, abstract=True):

	# Reference to unit definitions of other auxiliary units
	# to allow aux units to be able to load other aux units
	aux: ClassVar[Dict[str, Any]] = {}

	class Config:
		extra = Extra.forbid
		arbitrary_types_allowed = True

	# ignore no-redef mypy warning caused by the TypeVar definition above
	@defaultmethod(check_super_call=True) # type: ignore[no-redef]
	def __init__(self, **kwargs):
		self.__config__.extra = Extra.forbid
		super().__init__(**kwargs)
		self.__config__.extra = Extra.allow
