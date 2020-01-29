#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/model/time/TimeStringModel.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 29.09.2018
# Last Modified Date: 29.01.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Literal
from datetime import datetime
from pydantic import BaseModel
from ampel.utils.docstringutils import gendocstring


@gendocstring
class TimeStringModel(BaseModel):

	matchType: Literal['timeString']
	dateTimeStr: str
	dateTimeFormat: str

	# pylint: disable=unused-argument
	def get_timestamp(self, **kwargs) -> float:
		""" """
		return datetime \
			.strptime(self.dateTimeStr, self.dateTimeFormat) \
			.timestamp()
