#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/model/time/TimeLastRunModel.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 29.09.2018
# Last Modified Date: 29.01.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Union, Dict, Optional, Literal
from ampel.model.StrictModel import StrictModel
from ampel.query.QueryEventsCol import QueryEventsCol
from ampel.db.AmpelDB import AmpelDB


class TimeLastRunModel(StrictModel):

	match_type: Literal['time_last_run']
	process_name: str
	fallback: Union[None, Dict] = None


	def get_timestamp(self, **kwargs) -> Optional[float]:
		return self._query_events_col(kwargs['db'], self)


	@staticmethod
	def _query_events_col(ampel_db: AmpelDB, model: 'TimeLastRunModel') -> Optional[float]:

		col = ampel_db.get_collection('events')

		# First query the last 10 days (default value for back_days in QueryEventsCol)
		res = next(
			col.aggregate(
				QueryEventsCol.get_last_run(model.process_name)
			), None
		)

		# If nothing is found, try querying the entire collection (days_back=None)
		if res is None:

			res = next(
				col.aggregate(
					QueryEventsCol.get_last_run(
						model.process_name, days_back=None
					)
				), None
			)

			if res is None:
				return None

		return res['events']['ts']
