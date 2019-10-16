#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/model/t3/T3TaskConfig.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 02.09.2018
# Last Modified Date: 10.10.2019
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import schedule as sched
from pydantic import validator
from typing import Dict, Union, List
from ampel.common.docstringutils import gendocstring
from ampel.model.AmpelBaseModel import AmpelBaseModel
from ampel.config.ScheduleEvaluator import ScheduleEvaluator
from ampel.model.legacy.stock.StockConfig import StockConfig
from ampel.model.UnitConfig import UnitConfig

@gendocstring
class T3TaskConfig(AmpelBaseModel):
	"""
	Example:
	"""
	task: str
	unit: UnitConfig
	schedule: Union[None, List[str]] = None
	verbose: bool = False
	globalInfo: bool = False
	transients: Union[None, StockConfig] = None
	repo: str = None


	@validator('schedule', pre=True, whole=True)
	def schedule_should_be_a_sequence(cls, schedule):
		"""
		"""
		# cast to sequence
		if type(schedule) is str:
			return (schedule,)

		return schedule


	@validator('schedule', whole=True)
	def schedule_must_not_contain_bad_things(cls, schedule):
		"""
		Safety check for "schedule" parameters 
		"""
		evaluator = ScheduleEvaluator()
		for el in schedule:
			try:
				evaluator(sched.Scheduler(), el).do(lambda x: None)
			except:
				raise ValueError("Bad 'schedule' parameter")

		return schedule
