#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/model/ProcessModel.py
# License:             BSD-3-Clause
# Author:              valery brinnel <firstname.lastname@gmail.com>
# Date:                06.10.2019
# Last Modified Date:  30.12.2021
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

import schedule as sched
from typing import Literal
from collections.abc import Sequence
from ampel.base.AmpelBaseModel import AmpelBaseModel
from ampel.types import ChannelId
from ampel.model.UnitModel import UnitModel
from ampel.config.ScheduleEvaluator import ScheduleEvaluator


class ProcessModel(AmpelBaseModel):

	name: str
	version: int | float | str
	active: bool = True
	tier: None | Literal[0, 1, 2, 3]
	schedule: Sequence[str]
	channel: None | ChannelId | Sequence[ChannelId]
	distrib: None | str
	source: None | str
	isolate: bool = True
	multiplier: int = 1
	log: None | str
	controller: UnitModel = UnitModel(unit='DefaultProcessController')
	processor: UnitModel


	def __init__(self, **kwargs) -> None:

		if isinstance(kwargs.get('schedule'), str):
			kwargs['schedule'] = [kwargs['schedule']]

		super().__init__(**kwargs)

		evaluator = None
		for el in self.schedule:
			if el == "super":
				continue
			try:
				if evaluator is None:
					evaluator = ScheduleEvaluator()
				evaluator(sched.Scheduler(), el).do(lambda x: None)
			except Exception:
				raise ValueError("Incorrect 'schedule' parameter")
