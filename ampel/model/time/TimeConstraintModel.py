#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/model/time/TimeConstraintModel.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 29.09.2018
# Last Modified Date: 06.06.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Union, Optional
from ampel.model.time.TimeDeltaModel import TimeDeltaModel
from ampel.model.time.TimeLastRunModel import TimeLastRunModel
from ampel.model.time.TimeStringModel import TimeStringModel
from ampel.model.time.UnixTimeModel import UnixTimeModel
from ampel.model.time.QueryTimeModel import QueryTimeModel
from ampel.model.StrictModel import StrictModel


class TimeConstraintModel(StrictModel):
	"""
	Examples::
	  
	  TimeConstraintModel(
	      **{
	          "after": {
	              "match_type": "time_delta",
	              "days": -1
	          },
	          "before": {
	              "match_type": "time_string",
	              "dateTimeStr": "21/11/06 16:30",
	              "dateTimeFormat": "%d/%m/%y %H:%M"
	          }
	      }
	  )

	::
	  
	  TimeConstraintModel(
	      **{
	          "after": {
	              "match_type": "time_last_run",
	              "name": "val_test"
	          },
	          "before": {
	              "match_type": "unix_time",
	              "value": 1531306299
	          }
	      }
	  )
	"""

	before: Optional[
		Union[
			TimeDeltaModel, TimeLastRunModel,
			TimeStringModel, UnixTimeModel
		]
	] = None

	after: Optional[
		Union[
			TimeDeltaModel, TimeLastRunModel,
			TimeStringModel, UnixTimeModel
		]
	] = None


	def get_query_model(self, **kwargs) -> Optional[QueryTimeModel]:
		"""
		Call this method with db=<instance of AmpelDB>
		if your time constraint is based on TimeLastRunModel
		"""

		if self.before is None and self.after is None:
			return None

		return QueryTimeModel(
			before = self.before.get_timestamp(**kwargs) if self.before else None,
			after = self.after.get_timestamp(**kwargs) if self.after else None
		)
