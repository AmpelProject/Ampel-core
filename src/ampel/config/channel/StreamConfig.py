#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/config/channel/StreamConfig.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 02.09.2018
# Last Modified Date: 07.12.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from pydantic import BaseModel, validator
from typing import Tuple, Dict, Union, Any, List
from ampel.config.AmpelModelExtension import AmpelModelExtension
from ampel.common.docstringutils import gendocstring
from ampel.config.channel.T0UnitConfig import T0UnitConfig
from ampel.config.channel.T1UnitConfig import T1UnitConfig
from ampel.config.channel.T2UnitConfig import T2UnitConfig
from ampel.config.t3.T3TaskConfig import T3TaskConfig

@gendocstring
class StreamConfig(AmpelModelExtension):
	"""
	Config holder for AMPEL channel input streams (ex: ZTFIPAC) 
	-> values defined in channel configuration section 'sources'

	Note: pydantic does not support typing.Sequence
	https://github.com/samuelcolvin/pydantic/issues/185
	We are thus forced to use Union[List, Tuple]

	Update: Sequence might be supported in future pydantic updates:
	https://github.com/samuelcolvin/pydantic/issues/304
	"""
	stream: str
	parameters: Union[None, Any] = None
	t0Filter: Union[None, T0UnitConfig] # None allowed bc of the t3 partial loading option
	t1Update: Union[None, List[T1UnitConfig], Tuple[T1UnitConfig]] = []
	t2Compute: Union[None, List[T2UnitConfig], Tuple[T2UnitConfig]] = []
	t3Supervise: Union[None, List[T3TaskConfig], Tuple[T3TaskConfig]] = []


	def __init__(self, **arg):

		# We allow - for convenience - t2Compute or t3Supervise to be defined as single dicts.
		# A cast into sequence (tuple) is necessary in this case (validator cast_to_tuple). 
		# Since a such cast modifies input, a shallow dict copy is necessary.
		if (
			isinstance(arg.get('t1Update'), dict) or
			isinstance(arg.get('t2Compute'), dict) or
			isinstance(arg.get('t3Supervise'), dict)
		):
			super().__init__(**dict(arg)) # shallow copy
		else:
			super().__init__(**arg)


	@validator('t1Update', 't3Supervise', pre=True, whole=True)
	def t0_partial_loading(cls, value):
		""" """
		return cls.partial_loading(0, value)


	@validator('t0Filter', 't2Compute', 't3Supervise', pre=True, whole=True)
	def t1_partial_loading(cls, value):
		""" """
		return cls.partial_loading(1, value)


	@validator('t0Filter', 't1Update', 't2Compute', pre=True, whole=True)
	def t3_partial_loading(cls, value):
		""" """
		return cls.partial_loading(3, value)


	@classmethod		
	def partial_loading(cls, tier, value):
		# StreamConfig.__tier__ is set upstream by ChannelConfig
		if hasattr(cls, "__tier__") and cls.__tier__ == tier:
			return None
		return value


	@validator('t2Compute', pre=True, whole=True)
	def cast_to_tuple(cls, arg):
		""" """
		if type(arg) is dict:
			return (arg,)
		return arg
