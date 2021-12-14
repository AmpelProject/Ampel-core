#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/core/ContextUnit.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 07.10.2019
# Last Modified Date: 13.12.2021
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Optional
from ampel.types import Traceless, TRACELESS
from ampel.base.AmpelBaseModel import AmpelBaseModel
from ampel.core.AmpelContext import AmpelContext
from ampel.secret.Secret import Secret

ttf = type(Traceless)


class ContextUnit(AmpelBaseModel):
	"""
	Base class for units requiring a reference to an AmpelContext instance
	"""

	context: Traceless[AmpelContext]

	#: Private variable potentially set by UnitLoader for provenance purposes. Either:
	#: * None if provanance flag is False
	#: * 0 in case model content is not serializable
	#: * any other signed int value
	_trace_id: Optional[int] = None


	def __init__(self, **kwargs) -> None:

		super().__init__(**kwargs)

		d = self.__dict__
		excl = {
			k for k, v in self._annots.items()
			if type(v) is ttf and v.__metadata__[0] == TRACELESS
		}

		self._trace_content = {
			k: d[k]
			for k in sorted(d)
			if k not in excl and
			not isinstance(d[k], Secret)
		}
