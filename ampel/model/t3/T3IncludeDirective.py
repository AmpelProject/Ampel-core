#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/model/t3/T3IncludeDirective.py
# License:             BSD-3-Clause
# Author:              valery brinnel <firstname.lastname@gmail.com>
# Date:                17.12.2021
# Last Modified Date:  17.12.2021
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

from ampel.types import OneOrMany
from ampel.model.UnitModel import UnitModel
from ampel.base.AmpelBaseModel import AmpelBaseModel


class T3IncludeDirective(AmpelBaseModel):
	"""
	:param session: models for AbsT3Supplier[dict] instances which populates the 'session' field of T3Store
	Examples of session information are:
	- Date and time the current process was last run
	- Number of alerts processed since then
	"""

	#: Provides Iterable[T3Document]
	docs: None | UnitModel = None

	#: Provides session information. Unit(s) must be a subclass of AbsT3Supplier
	session: None | OneOrMany[UnitModel] = None
