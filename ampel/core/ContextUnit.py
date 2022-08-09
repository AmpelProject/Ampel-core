#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/core/ContextUnit.py
# License:             BSD-3-Clause
# Author:              valery brinnel <firstname.lastname@gmail.com>
# Date:                07.10.2019
# Last Modified Date:  09.01.2022
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

from ampel.types import Traceless
from ampel.base.AmpelUnit import AmpelUnit
from ampel.core.AmpelContext import AmpelContext


class ContextUnit(AmpelUnit):
	"""
	Base class for units requiring a reference to an AmpelContext instance
	"""

	context: Traceless[AmpelContext]

	#: Private variable potentially set by UnitLoader for provenance purposes. Either:
	#: * None if provanance flag is False
	#: * 0 in case model content is not serializable
	#: * any other signed int value
	_trace_id: None | int = None
