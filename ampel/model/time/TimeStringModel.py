#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/model/time/TimeStringModel.py
# License:             BSD-3-Clause
# Author:              valery brinnel <firstname.lastname@gmail.com>
# Date:                29.09.2018
# Last Modified Date:  06.06.2020
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

from typing import Literal
from datetime import datetime
from ampel.base.AmpelBaseModel import AmpelBaseModel


class TimeStringModel(AmpelBaseModel):

	match_type: Literal['time_string']
	dateTimeStr: str
	dateTimeFormat: str

	def get_timestamp(self, **kwargs) -> float:

		return datetime \
			.strptime(self.dateTimeStr, self.dateTimeFormat) \
			.timestamp()
