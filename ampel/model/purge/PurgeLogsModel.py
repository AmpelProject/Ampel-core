#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/model/purge/PurgeLogsModel.py
# License:             BSD-3-Clause
# Author:              valery brinnel <firstname.lastname@gmail.com>
# Date:                18.06.2020
# Last Modified Date:  18.06.2020
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

from typing import Literal
from ampel.base.AmpelBaseModel import AmpelBaseModel


class PurgeLogsModel(AmpelBaseModel):
	"""
	:param delay: time of inactivity in days after which logs will be purged
	:param format: output format exported by ampel.
	- bson: (native format) use this format if you plan on working on archives with your own mongodb
	- json: the ObjectId '_id' will be replaced by the (embbed) log timestamp
	- csv: the ObjectId '_id' will be replaced by the (embbed) log timestamp
	:param compress: whether to compress the output file
	"""
	delay: int
	format: Literal['bson', 'json', 'csv']
	compress: bool = True
	header: bool = False
