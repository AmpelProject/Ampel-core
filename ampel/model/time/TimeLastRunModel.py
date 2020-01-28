#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/model/time/TimeLastRunModel.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 29.09.2018
# Last Modified Date: 10.12.2019
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from pydantic import constr
from typing import Union, Dict, Optional
from ampel.db.AmpelDB import AmpelDB
from ampel.utils.docstringutils import gendocstring
from ampel.model.AmpelBaseModel import AmpelBaseModel
from ampel.query.QueryEventsCol import QueryEventsCol

@gendocstring
class TimeLastRunModel(AmpelBaseModel):

	matchType: constr(regex='^timeLastRun$')
	processName: str
	fallback: Union[None, Dict] = None

	# pylint: disable=unused-argument
	def get_timestamp(self, **kwargs) -> Optional[float]:
		""" """
		return self._query_events_col(kwargs['ampelDB'], self)


	@staticmethod
	def _query_events_col(ampel_db: AmpelDB, model: 'TimeLastRunModel') -> float:

		col = ampel_db.get_collection('events')

		# First query the last 10 days (default value for back_days in QueryEventsCol)
		res = next(
			col.aggregate(
				QueryEventsCol.get_last_run(model.processName)
			), None
		)

		# If nothing is found, try querying the entire collection (days_back=None)
		if res is None:

			res = next(
				col.aggregate(
					QueryEventsCol.get_last_run(
						model.processName, days_back=None
					)
				), None
			)

			if res is None:
				return None

		return res['events']['ts']
