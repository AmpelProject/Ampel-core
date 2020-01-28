#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/model/time/TimeStringModel.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 29.09.2018
# Last Modified Date: 10.12.2019
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Optional
from datetime import datetime
from pydantic import BaseModel, constr

from ampel.utils.docstringutils import gendocstring

@gendocstring
class TimeStringModel(BaseModel):

	matchType: constr(regex='^timeString$')
	dateTimeStr: str
	dateTimeFormat: str

	# pylint: disable=unused-argument
	def get_timestamp(self, **kwargs) -> Optional[float]:
		""" """
		return datetime.strptime(self.dateTimeStr, self.dateTimeFormat)
