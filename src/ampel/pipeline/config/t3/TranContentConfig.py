#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/config/t3/T3TranLoadConfig.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 29.09.2018
# Last Modified Date: 29.09.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from pydantic import BaseModel, validator
from typing import Dict, Union, List
from ampel.pipeline.common.docstringutils import gendocstring
from ampel.pipeline.config.t3.T3LoadableDocs import T3LoadableDocs

@gendocstring
class T3TranLoadConfig(BaseModel):
	""" """
	state: str = "$latest"
	docs: Union[T3LoadableDocs, List[T3LoadableDocs]] = None
	t2s: Union[None, str, List[str]] = None
	verbose: bool = True
	debug: bool = False


	@validator('state')
	def validate_state(cls, v):
		"""
		"""
		if v != "$latest" and v != "$all":
			raise ValueError('Parameter "state" must be either "$latest" of "$all"')

		return v
