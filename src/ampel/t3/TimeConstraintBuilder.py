#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/t3/TimeConstraintBuilder.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 06.06.2018
# Last Modified Date: 16.10.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Union
from datetime import datetime, timedelta

from ampel.db.AmpelDB import AmpelDB
from ampel.model.time.TimeDeltaModel import TimeDeltaModel
from ampel.model.time.TimeLastRunModel import TimeLastRunModel
from ampel.model.time.TimeStringModel import TimeStringModel
from ampel.model.time.UnixTimeModel import UnixTimeModel
from ampel.model.time.TimeConstraintModel import TimeConstraintModel


class TimeConstraintBuilder:
	"""
	"""

	def __init__(self, ampel_db: AmpelDB, tc_config: TimeConstraintModel = None):
		"""
		:param tc_config: if not provided in constructor, 
		please make sure to call method :func:`set <ampel.t3.TimeConstraintBuilder.set>` 
		"""

		self.constraints = {}
		self._ampel_db = ampel_db

		if tc_config is not None:
			self.set("before", tc_config.before)
			self.set("after", tc_config.after)


	def has_constraint(self) -> bool:
		""" """ 
		return len(self.constraints) > 0


	def get_after(self) -> datetime:
		""" """ 
		return self.get('after')
	

	def get_before(self) -> datetime:
		""" """ 
		return self.get('before')


	def set(self, 
		constraint_name: str, 
		value: Union[
			type(None), datetime, timedelta, TimeDeltaModel, 
			TimeLastRunModel, UnixTimeModel, TimeStringModel
		]
	) -> None:
		""" 
		:param constraint_name: "before" or "after"
		:type value: datetime, timedelta, TimeDeltaModel, \
		TimeLastRunModel, UnixTimeModel, TimeStringModel
		""" 

		if constraint_name not in ["before", "after"]:
			raise ValueError("constraint_name must be 'before' or 'after'")

		if not isinstance(value, [
			type(None), datetime, timedelta, TimeDeltaModel, 
			TimeLastRunModel, UnixTimeModel, TimeStringModel
		]): 
			raise ValueError(f"Unsupported type ({type(value)})")

		self.constraints[constraint_name] = value


	def get(self, param: str) -> datetime:
		""" 
		Schema validation ensures value can be only either None, dict, datetime or timedelta
		:param param: either 'after' or 'before'
		"""

		tc = self.constraints.get(param, None)

		if tc is None:
			return None

		if isinstance(tc, datetime):
			return tc

		if isinstance(tc, timedelta):
			return datetime.today() + tc

		if isinstance(tc, TimeDeltaModel):
			return datetime.today() + timedelta(**tc.arguments)

		if isinstance(tc, TimeLastRunModel):

			from ampel.query.QueryEventsCol import QueryEventsCol
			col = self._ampel_db.get_collection('events')
			res = next(
				col.aggregate(
					QueryEventsCol.get_last_run(tc.event)
				),
				None
			)

			if res is None:
				res = next(
					col.aggregate(
						QueryEventsCol.get_last_run(
							tc.event, days_back=None
						)
					), 
					None
				)
				if res is None:
					return None

			return datetime.fromtimestamp(
				res['events']['dt']
			)

		if isinstance(tc, UnixTimeModel):
			return datetime.fromtimestamp(tc.value)

		if isinstance(tc, TimeStringModel):
			return datetime.strptime(tc.dateTimeStr, tc.dateTimeFormat)

		raise ValueError(f"Illegal argument (type: {type(tc)})")
