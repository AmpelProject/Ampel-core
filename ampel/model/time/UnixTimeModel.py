#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/model/time/UnixTimeModel.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 29.09.2018
# Last Modified Date: 29.01.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Literal
from pydantic import BaseModel
from ampel.utils.docstringutils import gendocstring


@gendocstring
class UnixTimeModel(BaseModel):

	matchType: Literal['unixTime']
	value: int

	# pylint: disable=unused-argument
	def get_timestamp(self, **kwargs) -> int:
		""" """
		return self.value
