#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/config/channel/StreamConfig.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 02.09.2018
# Last Modified Date: 09.10.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from pydantic import BaseModel, validator
from typing import Tuple, Dict, Union, Any, List
from ampel.pipeline.common.docstringutils import gendocstring
from ampel.pipeline.config.channel.T0UnitConfig import T0UnitConfig
from ampel.pipeline.config.channel.T2UnitConfig import T2UnitConfig

@gendocstring
class StreamConfig(BaseModel):
	"""
	Config holder for AMPEL channel input streams (ex: ZTFIPAC) 
	-> values defined in channel configuration section 'sources'
	"""
	stream: str
	parameters: Union[None, Any] = None
	t0Filter: T0UnitConfig
	# pydantic does not support typing.Sequence
	t2Compute: Union[None, List[T2UnitConfig], Tuple[T2UnitConfig]] = None

	@validator('t2Compute', pre=True, whole=True)
	def cast_to_tuple(cls, arg):
		""" """
		if type(arg) is dict:
			return (arg,)
		return arg
