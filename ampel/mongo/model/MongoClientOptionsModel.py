#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/model/db/MongoClientOptionsModel.py
# License:             BSD-3-Clause
# Author:              Jakob van Santen <jakob.van.santen@desy.de>
# Date:                08.11.2020
# Last Modified Date:  08.11.2020
# Last Modified By:    Jakob van Santen <jakob.van.santen@desy.de>

from ampel.base.AmpelBaseModel import AmpelBaseModel

class MongoClientOptionsModel(AmpelBaseModel):

	# 0 means use operating system's default socket timeout
	socketTimeoutMS: int = 0

	# 0 means use operating system's default socket timeout
	connectTimeoutMS: int = 0

	# https://github.com/mongodb/specifications/blob/master/source/server-selection/server-selection.rst#serverselectiontimeoutms
	serverSelectionTimeoutMS: int = 30000 # default is 30,000 (milliseconds)
