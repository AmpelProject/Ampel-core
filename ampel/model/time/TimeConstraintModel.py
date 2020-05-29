#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/model/time/TimeConstraintModel.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 29.09.2018
# Last Modified Date: 29.01.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Union, Optional
from ampel.util.docstringutils import gendocstring
from ampel.model.time.TimeDeltaModel import TimeDeltaModel
from ampel.model.time.TimeLastRunModel import TimeLastRunModel
from ampel.model.time.TimeStringModel import TimeStringModel
from ampel.model.time.UnixTimeModel import UnixTimeModel
from ampel.model.time.QueryTimeModel import QueryTimeModel
from ampel.model.AmpelStrictModel import AmpelStrictModel


@gendocstring
class TimeConstraintModel(AmpelStrictModel):
	"""
	example1:

	TimeConstraintModel(
		**{
			"after": {
				"match_type": "timeDelta",
				"days": -1
			},
			"before": {
				"match_type": "timeString",
				"dateTimeStr": "21/11/06 16:30",
				"dateTimeFormat": "%d/%m/%y %H:%M"
			}
		}
	)

	example2:

	TimeConstraintModel(
		**{
			"after": {
				"match_type": "timeLastRun",
				"name": "val_test"
			},
			"before": {
				"match_type": "unixTime",
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
