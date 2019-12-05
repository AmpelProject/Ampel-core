#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/model/builder/ModelGeneralDistConf.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 09.10.2019
# Last Modified Date: 10.10.2019
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import List, Dict, Any, Optional, Union
from pydantic import validator
from ampel.model.AmpelBaseModel import AmpelBaseModel
from ampel.model.ProcessModel import ProcessModel
from ampel.model.builder.BuilderAliasModel import BuilderAliasModel
from ampel.model.builder.ChanT3Process1 import ChanT3Process1
from ampel.model.builder.ChanT3Process3 import ChanT3Process3


class ModelGeneralDistConf(AmpelBaseModel):

	channel: Optional[List[Dict[str, Any]]]
	unit: Optional[List[str]]
	controller: Optional[List[str]]
	executor: Optional[List[str]]
	process: Optional[List[Union[ChanT3Process1, ChanT3Process3, ProcessModel]]]
	alias: Optional[BuilderAliasModel]

	@validator('channel', 'process', pre=True, whole=True)
	def cast_to_list(cls, value):
		if not isinstance(value, List):
			return [value]

		return value
