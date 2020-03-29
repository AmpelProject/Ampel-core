#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/model/ProcessModel.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 06.10.2019
# Last Modified Date: 19.02.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import schedule as sched
from pydantic import validator
from typing import Sequence, Optional, Literal
from ampel.types import ProcUnitModels
from ampel.model.AmpelStrictModel import AmpelStrictModel
from ampel.types import ChannelId
from ampel.model.PlainUnitModel import PlainUnitModel
from ampel.config.ScheduleEvaluator import ScheduleEvaluator


class ProcessModel(AmpelStrictModel):

	schedule: Sequence[str]
	tier: Literal[0, 1, 2, 3]
	name: Optional[str]
	active: bool = True
	controller: PlainUnitModel
	processor: ProcUnitModels
	distrib: Optional[str]
	source: Optional[str]
	channel: Optional[ChannelId]


	@validator('name')
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
