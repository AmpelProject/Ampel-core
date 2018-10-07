#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/config/t3/T3TaskConfig.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 02.09.2018
# Last Modified Date: 30.09.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from pydantic import BaseModel, validator
from typing import Dict, Any, Union, List
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
	schedule: Union[None, str, List[str]] = None
	verbose: bool = False
	transients: Union[None, TranConfig] = None
	runConfig: Union[None, dict] = None


	@validator('runConfig')
	def validate_unit_run_config(cls, run_config, values, **kwargs):
		"""
		"""

		T3UnitClass = AmpelUnitLoader.get_class(
			tier=3, unit_name=values['unitId']
		)

		if T3UnitClass is None:
			raise ValueError("Unable to load T3 unit %s" % values['unitId'])

		if hasattr(T3UnitClass, 'RunConfig'):
			return getattr(T3UnitClass, 'RunConfig')(**run_config)

		return run_config
