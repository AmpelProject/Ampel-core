#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/config/t3/T3TaskConfig.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 02.09.2018
# Last Modified Date: 30.09.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from pydantic import BaseModel, validator
from typing import Dict, Any, Union
from ampel.pipeline.common.docstringutils import gendocstring
from ampel.pipeline.config.t3.T3TranConfig import T3TranConfig
from ampel.pipeline.t3.T3TaskBody import T3TaskBody


@gendocstring
class T3TaskConfig(BaseModel):
	"""
	"""
	task: str
	unitId: str
	verbose: bool = False
	transients: Union[None, T3TranConfig] = None
	runConfig: Union[None, dict] = None


	@validator('runConfig')
	def validate_unit_run_config(cls, run_config, values, **kwargs):
		"""
		"""
		if run_config is None:
			return

		t3_unit = T3TaskBody.get_t3_unit(values['unitId'])

		if hasattr(t3_unit, 'RunConfig'):
			return getattr(t3_unit, 'RunConfig')(**run_config)

		return run_config
