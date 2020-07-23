#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/model/template/LegacyT3Process2.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 09.10.2019
# Last Modified Date: 06.02.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import List, Dict, Any
from ampel.model.legacy.BaseT3Process import BaseT3Process
from ampel.util.docstringutils import gendocstring
from ampel.model.StrictModel import StrictModel


@gendocstring
class TaskData(StrictModel):
	"""
	Example:
	"""
	name: str
	unit: str
	config: Dict[str, Any]


@gendocstring
class LegacyT3Process2(BaseT3Process):
	"""
	Handles the following formats:
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
			},
			{
				"unit": "MarshalPublisher",
				"config": {
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
			"name": d['name'],
			"distrib": d['distrib'],
			"schedule": d['schedule'],
			"controller": {
				"unit": "T3Controller"
			},
			"processor": {
				"unit": "T3MultiUnitExecutor",
				"config": {
					"transients": tran,
					"task": d['task'].dict(skip_defaults=True)
				}
			}
		}
