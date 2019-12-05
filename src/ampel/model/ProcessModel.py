#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/model/ProcessModel.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 06.10.2019
# Last Modified Date: 11.10.2019
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import schedule as sched
from pydantic import validator
from typing import Sequence, Union, Optional
from ampel.common.docstringutils import gendocstring
from ampel.model.AmpelBaseModel import AmpelBaseModel
from ampel.model.UnitModel import UnitModel
from ampel.config.ScheduleEvaluator import ScheduleEvaluator

@gendocstring
class ProcessModel(AmpelBaseModel):

	schedule: Sequence[str]
	tier: int
	processName: Optional[str]
	controller: UnitModel
	processor: UnitModel
	distName: Optional[str] = None
	channel: Optional[Union[int, str]] = None


	@validator('processName')
	def t3_processes_must_define_process_name(cls, v, values):
		if values['tier'] == 3 and not v:
			raise ValueError("T3 processes must define a process name")
		return v


	@validator('schedule', pre=True, whole=True)
	def cast_to_list(cls, v):
		if isinstance(v, str):
			return (v, )
		return v


	@validator('schedule', whole=True)
	def schedule_must_not_contain_bad_things(cls, schedule):
		"""
		Safety check for "schedule" parameters 
		"""
		evaluator = ScheduleEvaluator()
		for el in schedule:
			if el == "super":
				continue
			try:
				evaluator(sched.Scheduler(), el).do(lambda x: None)
			except Exception:
				raise ValueError("Incorrect 'schedule' parameter")

		return schedule
