#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/model/ProcessModel.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 06.10.2019
# Last Modified Date: 07.04.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import schedule as sched
from pydantic import validator
from typing import Sequence, Optional, Literal, Union, Dict, Any
from ampel.model.AmpelStrictModel import AmpelStrictModel
from ampel.type import ChannelId
from ampel.model.UnitModel import UnitModel
from ampel.config.ScheduleEvaluator import ScheduleEvaluator


class ProcessModel(AmpelStrictModel):

	name: str
	active: bool = True
	tier: Literal[0, 1, 2, 3]
	schedule: Sequence[str]
	channel: Optional[ChannelId]
	distrib: Optional[str]
	source: Optional[str]
	isolate: bool = True
	multiplier: int = 1
	logger: Optional[Union[str, Dict[str, Any]]]
	controller: UnitModel = UnitModel(unit='DefaultProcessController')
	processor: UnitModel


	@validator('schedule', pre=True, whole=True)
	def _cast_to_list(cls, v):
		if isinstance(v, str):
			return (v, )
		return v


	@validator('schedule', whole=True)
	def _check_schedule_validity(cls, schedule):
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
