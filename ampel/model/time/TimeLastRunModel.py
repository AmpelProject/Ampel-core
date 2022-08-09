#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/model/time/TimeLastRunModel.py
# License:             BSD-3-Clause
# Author:              valery brinnel <firstname.lastname@gmail.com>
# Date:                29.09.2018
# Last Modified Date:  31.07.2020
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

from typing import Literal
from datetime import datetime, timedelta
from ampel.base.AmpelBaseModel import AmpelBaseModel
from ampel.mongo.query.var.events import get_last_run
from ampel.core.AmpelDB import AmpelDB


class TimeLastRunModel(AmpelBaseModel):

	match_type: Literal['time_last_run']
	process_name: str
	fallback: None | dict = {'days': -1}
	require_success: bool = True


	def get_timestamp(self, **kwargs) -> None | float:
		if ts := self._query_events_col(kwargs['db'], self):
			return ts
		if self.fallback:
			return (datetime.today() + timedelta(**self.fallback)).timestamp()
		return None


	@staticmethod
	def _query_events_col(ampel_db: AmpelDB, model: 'TimeLastRunModel') -> None | float:

		col = ampel_db.get_collection('events')

		# First query the last 10 days
		res = get_last_run(
			col, model.process_name, model.require_success,
			gte_time=(datetime.today() - timedelta(days=10)).timestamp(),
			timestamp = True,
		)

		# If nothing is found, try querying the entire collection (days_back=None)
		if res is None:
			res = get_last_run(col, model.process_name, model.require_success, gte_time=None, timestamp=True)
			if not res:
				return None

		return res
