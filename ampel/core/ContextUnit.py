#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/core/ContextUnit.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 07.10.2019
# Last Modified Date: 19.09.2021
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Optional
from ampel.base.AmpelBaseModel import AmpelBaseModel
from ampel.core.AmpelContext import AmpelContext
from ampel.log.AmpelLogger import AmpelLogger
from ampel.secret.Secret import Secret


class ContextUnit(AmpelBaseModel):
	"""
	Base class for units requiring a reference to an AmpelContext instance
	"""

	#: Private variable potentially set by UnitLoader for provenance purposes. Either:
	#: * None if provanance flag is False
	#: * 0 in case model content is not serializable
	#: * any other signed int value
	_trace_id: Optional[int] = None

	def __init__(self, context: AmpelContext, **kwargs) -> None:

		if context is None:
			raise ValueError("Parameter context cannot be None")

		super().__init__(**kwargs)

		d = self.__dict__
		self._trace_content = {
			k: d[k]
			for k in sorted(d)
			if not isinstance(d[k], (Secret, AmpelContext, AmpelLogger))
		}

		self.context = context
