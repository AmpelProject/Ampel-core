#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/model/time/TimeLastRunModel.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 29.09.2018
# Last Modified Date: 20.06.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Union, Dict, Optional, Literal
from datetime import datetime, timedelta
from ampel.model.StrictModel import StrictModel
from ampel.db.query.events import get_last_run
from ampel.db.AmpelDB import AmpelDB


class TimeLastRunModel(StrictModel):

	match_type: Literal['time_last_run']
	process_name: str
	fallback: Union[None, Dict] = None


	def get_timestamp(self, **kwargs) -> Optional[float]:
		if ts := self._query_events_col(kwargs['db'], self):
			return ts
		if self.fallback:
			return (datetime.today() + timedelta(**self.fallback)).timestamp()
		return None


	@staticmethod
	def _query_events_col(ampel_db: AmpelDB, model: 'TimeLastRunModel') -> Optional[float]:

		col = ampel_db.get_collection('events')

		# First query the last 10 days
		res = next(
			col.aggregate(
				get_last_run(
					col, model.process_name,
					gte_time=(datetime.today() - timedelta(days=10)).timestamp()
				)
			), None
		)

		# If nothing is found, try querying the entire collection (days_back=None)
		if res is None:
			res = next(col.aggregate(get_last_run(col, model.process_name)), None)
			if not res:
				return None

		return res['events']['ts']
