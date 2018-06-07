#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/t3/TimeConstraint.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 06.06.2018
# Last Modified Date: 06.06.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from datetime import datetime, timedelta
from voluptuous import Schema, Any, Required


class TimeConstraint:
	"""
	-> provide either 'timeDelta' or ('from' and/or 'until')
	* 'timeDelta': instance of datetime.timedelta or dict instance 
	   that will be passed to datetime.timedelta (ex: {'days': -10})
	* 'from' and 'until': either datetime.datetime or dict fulfilling 
	   TimeConstraint.schema_types criteria
	"""

	_schema_time = Schema(
		Any(
			{Required('unixTime'): float},
			{Required('dateTimeStr'): str, Required('dateTimeFormat'): str}
		)
	)

	schema_types = Schema(
		Any(
			None,
			{
				"timeDelta": Any(None, timedelta, dict), 
				"from": Any(None, datetime, _schema_time), 
				"until": Any(None, datetime, _schema_time)
			}
		)
	)


	def __init__(self, parameters=None):
		"""
		paramters: dict instance with keys: 'timeDelta' or ('from' and/or 'until')
		* 'timeDelta' dict value: either datetime.timedelta or dict that will be 
		   passed to datetime.timedelta (ex: {'days': -10}
		* 'from' and 'until' dict values: either datetime.datetime 
		   or dict fulfilling TimeConstraint.schema_types criteria
		"""
		self.contraints = {}

		if parameters is not None:
			self.from_parameters(parameters, self)


	def set_time_delta(self, time_delta):
		"""	
		"""

		if 'from' in self.contraints or 'until' in self.contraints:
			raise ValueError("Time delta and time from/until cannot be used together")

		if type(time_delta) is dict: 
			self.contraints['timeDelta'] = timedelta(**time_delta)
		elif type(time_delta) is timedelta:
			self.contraints['timeDelta'] = time_delta
		else:
			raise ValueError("Illegal Argument")


	def set_time_contraint(self, key, value, schema_check=True):
		"""	
		key: either 'from' or 'until'
		"""

		if 'timeDelta' in self.contraints:
			raise ValueError("Time delta and time from/until cannot be used together")

		if key not in ['from', 'until']:
			raise ValueError("Key must be either 'from' or 'until'")

		if type(value) is dict: 
			if schema_check:
				TimeConstraint._schema_time(value)
			self.contraints[key] = TimeConstraint._get_time(value)
		elif type(value) is datetime:
			self.contraints[key] = value
		else:
			raise ValueError("Illegal Argument")


	def get(self, attr):
		"""
		attr: either 'timeDelta', 'from' or 'until'
		"""
		if attr == 'timeDelta' and self.contraints.get('timeDelta') is not None:
			return datetime.today() + self.contraints['timeDelta']

		return self.contraints.get(attr)


	@staticmethod
	def from_parameters(time_constraint_dict, time_constraint=None):
		"""	
		"""	
		if time_constraint_dict is None:
			return None

		# Robustness
		TimeConstraint.schema_types(time_constraint_dict)

		tc = TimeConstraint() if time_constraint is None else time_constraint

		if time_constraint_dict.get('timeDelta') is not None:
			tc.set_time_delta(time_constraint_dict['timeDelta'])

		for key in ('from', 'until'):
			if time_constraint_dict.get(key) is not None:
				tc.set_time_contraint(key, time_constraint_dict[key], schema_check=False)

		return tc


	@staticmethod
	def _get_time(val):
		"""
		Schema validation ensures val can be only either None, dict or datetime
		"""
		# Past this point, schema validation ensures it is dict 
		if 'unixTime' in val:
			return datetime.utcfromtimestamp(val['unixTime'])

		if 'dateTimeStr' in val:
			# Schema validation ensures 'dateTimeFormat' is set when 'dateTimeStr' is
			return datetime.strptime(val['dateTimeStr'], ['dateTimeFormat'])

		raise ValueError("Illegal arguments")
