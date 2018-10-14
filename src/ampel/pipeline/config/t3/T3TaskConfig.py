#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/config/t3/T3TaskConfig.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 02.09.2018
# Last Modified Date: 14.10.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from pydantic import BaseModel, validator
from typing import Dict, Union, List
from ampel.pipeline.common.AmpelUnitLoader import AmpelUnitLoader
from ampel.pipeline.common.docstringutils import gendocstring
from ampel.pipeline.config.AmpelModelExtension import AmpelModelExtension
from ampel.pipeline.config.t3.TranConfig import TranConfig

@gendocstring
class T3TaskConfig(BaseModel, AmpelModelExtension):
	"""
	Example:
	"""
	task: str
	unitId: str
	schedule: Union[None, List[str]] = None
	verbose: bool = False
	globalInfo: bool = False
	transients: Union[None, TranConfig] = None
	runConfig: Union[None, Dict] = None


	@validator('unitId', pre=True)
	def validate_t3_unit(cls, unit_id, values, **kwargs):
		"""
		"""
		if AmpelUnitLoader.get_class(tier=3, unit_name=unit_id) is None:
			cls.print_and_raise(
				header="T3 task config error",
				msg="Unable to find T3 unit %s" % unit_id
			)

		return unit_id


	@validator('runConfig')
	def validate_run_config(cls, run_config, values, **kwargs):
		"""
		"""

		# Exists (checked by prior validator)
		T3UnitClass = AmpelUnitLoader.get_class(
			tier=3, unit_name=values['unitId']
		)

		if hasattr(T3UnitClass, 'RunConfig'):
			return getattr(T3UnitClass, 'RunConfig')(**run_config)

		return run_config
