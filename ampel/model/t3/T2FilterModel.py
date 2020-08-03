#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/model/t3/T2FilterModel.py
# License           : BSD-3-Clause
# Author            : Jakob van Santen <jakob.van.santen@desy.de>
# Date              : 02.08.2020
# Last Modified Date: 02.08.2020
# Last Modified By  : Jakob van Santen <jakob.van.santen@desy.de>

from typing import Any, Dict
from ampel.model.StrictModel import StrictModel

class T2FilterModel(StrictModel):

	unit: str
	match: Dict[str, Any]