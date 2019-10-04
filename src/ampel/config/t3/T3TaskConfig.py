#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/src/ampel/config/t3/T3TaskConfig.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 02.09.2018
# Last Modified Date: 05.10.2019
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from pydantic import validator
from typing import Dict, Union, List
from ampel.common.docstringutils import gendocstring
from ampel.config.AmpelModelExtension import AmpelModelExtension
from ampel.config.t3.TranConfig import TranConfig
from ampel.config.UnitConfig import UnitConfig

@gendocstring
class T3TaskConfig(AmpelModelExtension):
	"""
	Example:
	"""
	task: str
	unit: UnitConfig
	schedule: Union[None, List[str]] = None
	verbose: bool = False
	globalInfo: bool = False
	transients: Union[None, TranConfig] = None
	repo: str = None


	@validator('schedule', pre=True, whole=True)
	def validate_schedule(cls, schedule):
		"""
		"""
		# cast to sequence
		if type(schedule) is str:
			return (schedule,)

		return schedule
