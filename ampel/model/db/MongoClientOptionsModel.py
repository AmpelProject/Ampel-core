#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/model/db/MongoClientOptionsModel.py
# License           : BSD-3-Clause
# Author            : Jakob van Santen <jakob.van.santen@desy.de>
# Date              : 08.11.2020
# Last Modified Date: 08.11.2020
# Last Modified By  : Jakob van Santen <jakob.van.santen@desy.de>

from typing import Optional
from ampel.model.StrictModel import StrictModel

class MongoClientOptionsModel(StrictModel):
	socketTimeoutMS: Optional[int]
	connectTimeoutMS: Optional[int]
	serverSelectionTimeoutMS: Optional[int]
