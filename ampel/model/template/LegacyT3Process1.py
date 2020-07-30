#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/model/template/LegacyT3Process1.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 09.10.2019
# Last Modified Date: 06.02.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from pydantic import validator
from typing import Union, List, Dict, Any, Optional
from ampel.model.legacy.BaseT3Process import BaseT3Process
from ampel.model.StrictModel import StrictModel
from ampel.util.docstringutils import gendocstring


@gendocstring
class TaskData(StrictModel):
	"""
	Example:
	"""
	name: Optional[str]
	unit: str
	config: Dict[str, Any]


@gendocstring
class LegacyT3Process1(BaseT3Process):
	"""
	Handles the two following possible formats:

	Simplest
	{
		"name": "RapidLowzMarshal",
		"tier": 3,
		"schedule": "every(10).minutes",
		"transients": {...},
		"unit": "MarshalPublisher",
		"config": {...}
	}

	Simple (only 1 task is accepted):
	{
		"name": "RapidLowzMarshal",
		"tier": 3,
		"schedule": "every(10).minutes",
		"transients": {...},
		"task": [
			{
				"unit": "MarshalPublisher",
				"config": {
					"marshal_program": "AmpelRapid"
				}
			}
		]
	}
	"""
	template: str
	unit: Optional[str]
	config: Optional[Dict[str, Any]]
	task: Optional[Union[TaskData, List[TaskData]]]


	@validator('task', pre=True, whole=True)
	def detect_further_variants_of_simplest_conf(cls, task, values):
		""" """
		if not task:
			return None

		if isinstance(task, list):
			if len(task) == 1:
				v = task[0]
			else:
				raise ValueError("ChanT3Process1 model not applicable (number of tasks != 1)")

		if isinstance(task, dict):
			v = task

		values['unit'] = v['unit']
		values['config'] = v['config']

		return None


	def get_process(self, channel: str) -> Dict[str, Any]:
		"""
		Override
		"""

		d = self.__dict__
		tran = d['transients'].dict(skip_defaults=True)
		tran['select']['channels'] = channel

		return {
			"tier": 3,
			"name": d['name'],
			"distrib": d['distrib'],
			"source": d['source'],
			"schedule": d['schedule'],
			"controller": {
				"unit": "T3Controller"
			},
			"processor": {
				"unit": "T3Processor",
				"config": {
					"context": [
						{'unit': 'T3AddLastRunTime'},
						{'unit': 'T3AddAlertsNumber'}
					],
					"select": {
						"unit": "T3StockSelector",
						"config": tran['select']
					},
					"load": {
						"unit": "T3SimpleDataLoader",
						"config": tran['content']
					},
					"run": {
						"unit": "T3UnitRunner",
						"config": d['config']
					} if not d.get('task') else d.get('task')
				}
			}
		}
