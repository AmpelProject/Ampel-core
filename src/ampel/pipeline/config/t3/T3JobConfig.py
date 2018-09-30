#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/config/T3JobConfig.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 29.09.2018
# Last Modified Date: 29.09.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import schedule
from pydantic import BaseModel, validator
from typing import Union, List
from ampel.pipeline.common.docstringutils import gendocstring
from ampel.pipeline.config.t3.ScheduleEvaluator import ScheduleEvaluator
from ampel.pipeline.config.t3.T3JobTranConfig import T3JobTranConfig
from ampel.pipeline.config.GettableConfig import GettableConfig
from ampel.pipeline.config.channel.T3TaskConfig import T3TaskConfig

def nothing():
	pass

@gendocstring
class T3JobConfig(BaseModel, GettableConfig):
	"""
	Possible 'schedule' values (https://schedule.readthedocs.io/en/stable/):
	"every(10).minutes"
	"every().hour"
	"every().day.at("10:30")"
	"every().monday"
	"every().wednesday.at("13:15")"
	"""
	job: str
	active: bool = True
	globalInfo: bool = False
	schedule: Union[str, List[str]]
	transients: Union[None, T3JobTranConfig]
	tasks: Union[T3TaskConfig, List[T3TaskConfig]]


	@validator('schedule')
	def schedule_must_not_contain_bad_things(cls, v):
		"""
		Safety check for "schedule" parameters 
		"""
		scheduler = schedule.Scheduler()
		evaluator = ScheduleEvaluator()
		for el in v if type(v) is str else [v]:
			evaluator(scheduler, v).do(nothing)
		return v
