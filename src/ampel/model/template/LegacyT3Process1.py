#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/model/template/LegacyT3Process1.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 09.10.2019
# Last Modified Date: 28.10.2019
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from pydantic import validator
from typing import Union, List, Dict, Any, Optional
from ampel.model.legacy.BaseT3Process import BaseT3Process
from ampel.model.AmpelBaseModel import AmpelBaseModel
from ampel.common.docstringutils import gendocstring


@gendocstring
class TaskData(AmpelBaseModel):
	"""
	Example:
	"""
	name: Optional[str]
	className: str
	initConfig: Dict[str, Any]


@gendocstring
class LegacyT3Process1(BaseT3Process):
	""" 
	Handles the two following possible formats:

	Simplest
	{
		"processName": "RapidLowzMarshal",
		"tier": 3,
		"schedule": "every(10).minutes",
		"transients": {...},
		"className": "MarshalPublisher",
		"initConfig": {...}
	}

	Simple (only 1 task is accepted):
	{
		"processName": "RapidLowzMarshal",
		"tier": 3,
		"schedule": "every(10).minutes",
		"transients": {...},
		"task": [
			{
				"className": "MarshalPublisher",
				"initConfig": {
					"marshal_program": "AmpelRapid"
				}
			}
		]
	}
	"""
	template: str
	className: str = None
	initConfig: Dict[str, Any] = None
	task: Optional[Union[TaskData, List[TaskData]]]


	@validator('task', pre=True, whole=True)
	def detect_further_variants_of_simplest_conf(cls, task, values):
		""" """
		if not task:
			return None

		if isinstance(task, List):
			if len(task) == 1:
				v = task[0]
			else:
				raise ValueError("ChanT3Process1 model not applicable (number of tasks != 1)")

		if isinstance(task, dict):
			v = task

		values['className'] = v['className']
		values['initConfig'] = v['initConfig']

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
			"processName": d['processName'],
			"distName": d['distName'],
			"schedule": d['schedule'],
			"controller": {
				"className": "T3Controller"
			},
			"processor": {
				"className": "T3MonoUnitExecutor",
				"initConfig": {
					"select": {
						"className": "T3StockSelector",
						"initConfig": tran['select']
					},
					"load": {
						"className": "DBContentLoader",
						"initConfig": tran['content']
					},
					"run": {
						"className": d['className'],
						"initConfig": d['initConfig']
					}
				}
			}
		}
