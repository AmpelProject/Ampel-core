#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/config/channel/ChannelConfig.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 02.09.2018
# Last Modified Date: 09.10.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from pydantic import BaseModel, validator
from typing import List, Sequence, Any, Union, Tuple
from ampel.pipeline.common.docstringutils import gendocstring
from ampel.pipeline.config.channel.StreamConfig import StreamConfig
from ampel.pipeline.config.t3.T3TaskConfig import T3TaskConfig
from ampel.pipeline.config.ReadOnlyDict import ReadOnlyDict

@gendocstring
class ChannelConfig(BaseModel):
	"""
	Config holder for AMPEL channels
	"""
	channel: str
	active: bool = True
	author: str = "Unspecified"
	# pydantic does not support typing.Sequence
	# https://github.com/samuelcolvin/pydantic/issues/185
	sources: Union[List[StreamConfig], Tuple[StreamConfig]]
	t3Supervise: Union[None, List[T3TaskConfig], Tuple[T3TaskConfig]] = None

	def __init__(self, **arg):

		# Because we allow - out of convenience - for sources or t3Supervise
		# to be a single dict (a cast into tuple happens then, which modifies input)
		if isinstance(arg['sources'], dict) or isinstance(arg.get('t3Supervise'), dict):
			super().__init__(**dict(arg)) # shallow copy
		else:
			super().__init__(**arg)


	@validator('sources', pre=True, whole=True)
	def cast_to_tuple(cls, sources):
		""" """
		if type(sources) is dict:
			return (sources,)
		return sources

	def get_stream_config(self, source):
		return next(filter(lambda x: x.stream==source, self.sources), None)
