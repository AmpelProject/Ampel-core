#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/model/time/TimeDeltaModel.py
# License:             BSD-3-Clause
# Author:              valery brinnel <firstname.lastname@gmail.com>
# Date:                29.09.2018
# Last Modified Date:  06.06.2020
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

from datetime import datetime, timedelta, timezone
from typing import Literal

from ampel.base.AmpelBaseModel import AmpelBaseModel


class TimeDeltaModel(AmpelBaseModel):

	match_type: Literal['time_delta']
	days: int = 0
	seconds: int = 0
	microseconds: int = 0
	milliseconds: int = 0
	minutes: int = 0
	hours: int = 0
	weeks: int = 0

	def get_timestamp(self, **kwargs) -> float:

		dt = (kwargs.get('now') or datetime.now(tz=timezone.utc)) + timedelta(
			days=self.days, seconds=self.seconds, microseconds=self.microseconds,
			milliseconds=self.milliseconds, minutes=self.minutes,
			hours=self.hours, weeks=self.weeks
		)

		return dt.timestamp()
