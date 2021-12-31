#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/model/t3/T2FilterModel.py
# License:             BSD-3-Clause
# Author:              Jakob van Santen <jakob.van.santen@desy.de>
# Date:                02.08.2020
# Last Modified Date:  02.08.2020
# Last Modified By:    Jakob van Santen <jakob.van.santen@desy.de>

from typing import Any
from ampel.base.AmpelBaseModel import AmpelBaseModel

class T2FilterModel(AmpelBaseModel):

	#: name of T2 unit
	unit: str
	#: Mongo match expression to apply to most recent result
	match: dict[str, Any]