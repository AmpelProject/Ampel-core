#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/model/time/UnixTimeModel.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 29.09.2018
# Last Modified Date: 06.06.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Literal
from pydantic import BaseModel


class UnixTimeModel(BaseModel):

	match_type: Literal['unix_time']
	value: int

	def get_timestamp(self, **kwargs) -> int:
		return self.value
