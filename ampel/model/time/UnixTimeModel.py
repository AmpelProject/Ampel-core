#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/model/time/UnixTimeModel.py
# License:             BSD-3-Clause
# Author:              valery brinnel <firstname.lastname@gmail.com>
# Date:                29.09.2018
# Last Modified Date:  06.06.2020
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

from typing import Literal
from ampel.base.AmpelBaseModel import AmpelBaseModel


class UnixTimeModel(AmpelBaseModel):

	match_type: Literal['unix_time']
	value: int

	def get_timestamp(self, **kwargs) -> int:
		return self.value
