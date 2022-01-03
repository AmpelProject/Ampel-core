#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/core/ContextUnit.py
# License:             BSD-3-Clause
# Author:              valery brinnel <firstname.lastname@gmail.com>
# Date:                07.10.2019
# Last Modified Date:  13.12.2021
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

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
	_trace_id: None | int = None


	def __init__(self, **kwargs) -> None:

		super().__init__(**kwargs)

		self._trace_content = dict(sorted(self.dict(exclude_unset=False, exclude_defaults=False).items()))
