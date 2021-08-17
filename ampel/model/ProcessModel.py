#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/model/ProcessModel.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 06.10.2019
# Last Modified Date: 04.03.2021
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import schedule as sched
from pydantic import validator
from typing import Sequence, Optional, Literal, Union
from ampel.model.StrictModel import StrictModel
from ampel.types import ChannelId
from ampel.model.UnitModel import UnitModel
from ampel.config.ScheduleEvaluator import ScheduleEvaluator


class ProcessModel(StrictModel):

	name: str
	version: Union[int, float, str]
	active: bool = True
	tier: Optional[Literal[0, 1, 2, 3]]
	schedule: Sequence[str]
	channel: Optional[Union[ChannelId, Sequence[ChannelId]]]
	distrib: Optional[str]
	source: Optional[str]
	isolate: bool = True
	multiplier: int = 1
	log: Optional[str]
	controller: UnitModel = UnitModel(unit='DefaultProcessController')
	processor: UnitModel


	@validator('schedule', pre=True, each_item=False)
	def _cast_to_list(cls, v):
		if isinstance(v, str):
			return [v]
		return v


	@validator('schedule', each_item=False)
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
