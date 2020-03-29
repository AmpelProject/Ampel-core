#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/model/template/LegacyT3Process3.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 09.10.2019
# Last Modified Date: 06.02.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from pydantic import validator
from typing import Any, List, Dict
from ampel.model.legacy.TranModel import TranModel
from ampel.model.legacy.BaseT3Process import BaseT3Process
from ampel.model.AmpelStrictModel import AmpelStrictModel
from ampel.utils.docstringutils import gendocstring


@gendocstring
class TaskData(AmpelStrictModel):
	"""
	Example:
	"""
	name: str
	transients: TranModel
	unit: str
	config: Dict[str, Any]

	@validator('transients')
	def transient_selection_must_not_contain_channel(cls, tran):
		if 'select' in tran and 'channel' in tran['select']:
			raise ValueError(
				"Channel selection not permitted for processes " +
				"embedded in channel definition. Offending dict: %s" % tran
			)
		return tran


@gendocstring
class LegacyT3Process3(BaseT3Process):
	"""
	"""
	task: List[TaskData]


	def get_process(self, channel: str) -> Dict[str, Any]:
		"""
		Override
		"""

		# Override!
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
			"executor": {
				"unit": "T3ComplexExecutor",
				"config": {
					"transients": tran,
					"task": [el.dict(skip_defaults=True) for el in d['task']]
				}
			}
		}
