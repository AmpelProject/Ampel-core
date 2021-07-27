#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/core/ContextUnit.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 07.10.2019
# Last Modified Date: 18.06.2021
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from ampel.base.AmpelBaseModel import AmpelBaseModel
from ampel.core.AmpelContext import AmpelContext
from ampel.log.AmpelLogger import AmpelLogger
from ampel.abstract.Secret import Secret
from ampel.util.mappings import dictify


class ContextUnit(AmpelBaseModel):
	"""
	Base class for units requiring a reference to an AmpelContext instance
	"""

	#: Private variable potentially set by UnitLoader for provenance purposes. Either:
	#: * 0 if provanance flag is False
	#: * -1 in case model content is not serializable
	#: * any other signed int value
	_trace_id: int = 0

	def __init__(self, context: AmpelContext, **kwargs) -> None:

		if context is None:
			raise ValueError("Parameter context cannot be None")

		super().__init__(**kwargs)

		self._trace_content = {
			k: dictify(v)
			for k, v in self.__dict__.items()
			if not isinstance(v, (Secret, AmpelContext, AmpelLogger))
		}

		self.context = context
