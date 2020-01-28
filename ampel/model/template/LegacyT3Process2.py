#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/model/template/LegacyT3Process2.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 09.10.2019
# Last Modified Date: 27.10.2019
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import List, Dict, Any
from ampel.model.legacy.BaseT3Process import BaseT3Process
from ampel.utils.docstringutils import gendocstring
from ampel.model.AmpelBaseModel import AmpelBaseModel


@gendocstring
class TaskData(AmpelBaseModel):
	"""
	Example:
	"""
	name: str
	className: str
	initConfig: Dict[str, Any]


@gendocstring
class LegacyT3Process2(BaseT3Process):
	""" 
	Handles the following formats:
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
			},
			{
				"className": "MarshalPublisher",
				"initConfig": {
					"marshal_program": "AmpelRapid"
				}
			}
		]
	}
	whereby tasks cannot defined their own transient/stock selection
	"""
	task: List[TaskData]

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
				"className": "T3MultiUnitExecutor",
				"initConfig": {
					"transients": tran,
					"task": d['task'].dict(skip_defaults=True)
				}
			}
		}
