#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/t3/TimeConstraint.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 06.06.2018
# Last Modified Date: 16.10.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from datetime import datetime, timedelta
from ampel.pipeline.config.AmpelConfig import AmpelConfig
from ampel.pipeline.db.AmpelDB import AmpelDB
from ampel.pipeline.config.time.TimeDeltaConfig import TimeDeltaConfig
from ampel.pipeline.config.time.TimeLastRunConfig import TimeLastRunConfig
from ampel.pipeline.config.time.TimeStringConfig import TimeStringConfig
from ampel.pipeline.config.time.UnixTimeConfig import UnixTimeConfig


class TimeConstraint:
	"""
	"""

	def __init__(self, tc_config=None):
		"""
		:param TimeConstraintConfig tc_config: TimeConstraintConfig instance (see docstring) 
		if not provided, please make sure to use the method 
		:func:`set <ampel.pipeline.t3.TimeConstraint.set>` 
		"""

		self.constraints = {}

		if tc_config is not None:
			self.set("before", tc_config.before)
			self.set("after", tc_config.after)


	def has_constraint(self):
		""" """ 
		return len(self.constraints) > 0


	def get_after(self):
		""" """ 
		return self.get('after')
	

	def get_before(self):
		""" """ 
		return self.get('before')


	def set(self, constraint_name, value):
		""" 
		:param str constraint_name: "before" or "after"
		:param value: "before" or "after"
		:type value: datetime or timedelta or TimeDeltaConfig or 
		TimeLastRunConfig or UnixTimeConfig or TimeStringConfig
		""" 

		if constraint_name not in ["before", "after"]:
			raise ValueError("constraint_name must be 'before' or 'after'")

		if type(value) not in [
			type(None), datetime, timedelta, TimeDeltaConfig, TimeLastRunConfig, 
			UnixTimeConfig, TimeStringConfig
		]: 
			raise ValueError("Unsupported type (%s)" % type(value))

		self.constraints[constraint_name] = value


	def get(self, param):
		""" 
		param: either 'after' or 'before'
		Schema validation ensures val can be only either None, dict, datetime or timedelta
		"""

		tc = self.constraints.get(param, None)

		if tc is None:
			return None

		tc_type = type(tc)

		if tc_type is datetime:
			return tc

		elif tc_type is timedelta:
			return datetime.today() + tc

		elif tc_type is TimeDeltaConfig:
			return datetime.today() + timedelta(**tc.arguments)

		elif tc_type is TimeLastRunConfig:

			from ampel.pipeline.db.query.QueryEventsCol import QueryEventsCol
			col = AmpelDB.get_collection('events')
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
							tc.jobName, days_back=None
						)
					), 
					None
				)
				if res is None:
					return None

			return datetime.fromtimestamp(res['events']['dt'])

		elif tc_type is UnixTimeConfig:
			return datetime.fromtimestamp(tc.value)

		elif tc_type is TimeStringConfig:
			return datetime.strptime(tc.dateTimeStr, tc.dateTimeFormat)

		raise ValueError("Illegal argument (type: %s)" % tc_type)
