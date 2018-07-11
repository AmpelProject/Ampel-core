#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/t3/TimeConstraint.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 06.06.2018
# Last Modified Date: 28.06.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from datetime import datetime, timedelta
from voluptuous import Schema, Any, Required
from types import MappingProxyType
from ampel.pipeline.config.AmpelConfig import AmpelConfig
from ampel.pipeline.db.AmpelDB import AmpelDB


class TimeConstraint:
	"""
	* 'after' and 'before': either:
		-> None
		-> datetime.datetime
		-> datetime.timedelta
		-> dict (fulfilling TimeConstraint._sub_schema criteria)

	dict examples fulfilling TimeConstraint._sub_schema criteria:

		Example 1:
			{
				"after": {
					"use": "$lastRunTime",
					"taskName": "val_test"
				},
				"before": {
					"use": "unixTime",
					"value": 1531306299
				}
			}

		Example 2:
			{
				"after": {
					"use": "$timeDelta",
					"arguments" :  {
                       	"days" : -40
                   	}
				},
				"before": {
					"use": "formattedString",
					"dateTimeStr": "21/11/06 16:30",
					"dateTimeFormat": "%d/%m/%y %H:%M"
				}
			}
	"""

	_sub_schema = Schema(
		Any(
			None, datetime, timedelta,
			{
				Required('use'): '$timeDelta',
				Required('arguments'): dict
			},
			{
				Required('use'): '$lastRunTime',
				Required('taskName'): str
			},
			{
				Required('use'): 'formattedString',
				Required('dateTimeStr'): str,
				Required('dateTimeFormat'): str
			},
			{
				Required('use'): 'unixTime',
				Required('value'): int
			}
		)
	)

	schema = Schema(
		Any(
			None,
			{
				"after": _sub_schema, 
				"before": _sub_schema
			}
		)
	)


	@staticmethod
	def from_parameters(
		time_constraint_dict, 
		time_constraint_obj=None, runs_col=None, force_schema_check=False
	):
		"""	
		"""	
		if time_constraint_dict is None:
			return None

		if AmpelConfig.is_frozen():
			if force_schema_check:
				from ampel.pipeline.common.DevUtils import DevUtils
				TimeConstraint.schema(
					DevUtils.recursive_unfreeze(
						time_constraint_dict
					)
				)
		else:
			TimeConstraint.schema(time_constraint_dict) # Robustness

		tc = TimeConstraint() if time_constraint_obj is None else time_constraint_obj
		tc.set_after(time_constraint_dict.get('after'), False)
		tc.set_before(time_constraint_dict.get('before'), False)
		return tc


	def __init__(self, parameters=None):
		"""
		parameters: dict instance with keys: 'after' and/or 'before'
		* 'after' and 'before' dict values, either: 
			-> datetime.datetime 
			-> datetime.timedelta 
		   	-> dict fulfilling TimeConstraint._sub_schema criteria (see class docstring)
		"""
		self.constraints = {}

		if parameters is not None:
			self.from_parameters(parameters, self)


	def has_constraint(self):
		""" """ 
		return len(self.constraints) > 0


	def set_after(self, value, check_schema=True):
		""" """
		self._set('after', value, check_schema)


	def set_before(self, value, check_schema=True):
		""" """
		self._set('before', value, check_schema)


	def get_after(self):
		""" """ 
		return self._get('after')
	

	def get_before(self):
		""" """ 
		return self._get('before')


	def _set(self, name, value, check_schema=True):
		""" """ 
		if check_schema:
			TimeConstraint._sub_schema(value)
		self.constraints[name] = value


	def _get(self, param):
		""" 
		param: either 'after' or 'before'
		Schema validation ensures val can be only either None, dict, datetime or timedelta
		"""

		constraint = self.constraints.get(param)

		if constraint is None:
			return None

		if type(constraint) is datetime:
			return constraint

		elif type(constraint) is timedelta:
			return datetime.today() + constraint

		elif type(constraint) in (dict, MappingProxyType):

			if constraint['use'] == '$timeDelta':
				return datetime.today() + timedelta(**constraint['arguments'])

			if constraint['use'] == '$lastRunTime':

				from ampel.pipeline.db.query.QueryLastJobRun import QueryLastJobRun
				col = AmpelDB.get_collection('runs')
				res = next(col.aggregate(constraint['jobName']), None)

				if res is None:
					res = next(col.aggregate(constraint['jobName'], back_days=None), None)
					if res is None:
						return None

				return datetime.fromtimestamp(res['jobs']['dt'])

			if constraint['use'] == 'unixTime':
				return datetime.fromtimestamp(constraint['value'])

			if constraint['use'] == 'formattedString':
				# Schema validation ensures 'dateTimeFormat' is set when 'dateTimeStr' is
				return datetime.strptime(
					constraint['dateTimeStr'], 
					constraint['dateTimeFormat']
				)

		raise ValueError("Illegal argument (type: %s)" % type(constraint))
