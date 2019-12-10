#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/model/time/TimeDeltaModel.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 29.09.2018
# Last Modified Date: 10.10.2019
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from pydantic import constr
from datetime import datetime, timedelta
from ampel.common.docstringutils import gendocstring
from ampel.model.AmpelBaseModel import AmpelBaseModel


@gendocstring
class TimeDeltaModel(AmpelBaseModel):

	matchType: constr(regex='^timeDelta$')
	days: int = 0
	seconds: int = 0 
	microseconds: int = 0
	milliseconds: int = 0
	minutes: int = 0
	hours: int = 0
	weeks: int = 0

	# pylint: disable=unused-argument
	def get_timestamp(self, **kwargs) -> float:
		""" """
		dt = datetime.today() + timedelta(
			days=self.days, seconds=self.seconds, microseconds=self.microseconds, 
			milliseconds=self.milliseconds, minutes=self.minutes, 
			hours=self.hours, weeks=self.hours
		)

		return dt.timestamp()
