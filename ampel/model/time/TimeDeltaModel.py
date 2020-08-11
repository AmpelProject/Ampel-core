#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/model/time/TimeDeltaModel.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 29.09.2018
# Last Modified Date: 06.06.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Literal
from datetime import datetime, timedelta
from ampel.model.StrictModel import StrictModel


class TimeDeltaModel(StrictModel):

	match_type: Literal['time_delta']
	days: int = 0
	seconds: int = 0
	microseconds: int = 0
	milliseconds: int = 0
	minutes: int = 0
	hours: int = 0
	weeks: int = 0

	def get_timestamp(self, **kwargs) -> float:

		dt = datetime.today() + timedelta(
			days=self.days, seconds=self.seconds, microseconds=self.microseconds,
			milliseconds=self.milliseconds, minutes=self.minutes,
			hours=self.hours, weeks=self.weeks
		)

		return dt.timestamp()
