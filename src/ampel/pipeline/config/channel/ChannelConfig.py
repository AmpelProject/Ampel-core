#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/config/ChannelConfig.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 02.09.2018
# Last Modified Date: 02.09.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from pydantic import BaseModel, validator
from typing import List, Dict, Any, Union
from ampel.pipeline.common.docstringutils import gendocstring
from ampel.pipeline.config.channel.StreamConfig import StreamConfig
from ampel.pipeline.config.t3.T3TaskConfig import T3TaskConfig

@gendocstring
class ChannelConfig(BaseModel):
	"""
	Config holder for AMPEL channels
	"""
	channel: str
	active: bool = True
	author: str = "Unspecified"
	sources: Union[StreamConfig, List[StreamConfig]]
	t3Supervise: Union[None, T3TaskConfig, List[T3TaskConfig]] = None

	@validator('sources', pre=True, whole=True)
	def make_it_a_list(cls, sources):
		""" """
		if type(sources) is not list:
			return [sources]
		return sources

	def get_stream_config(self, source):
		return next(filter(lambda x: x.stream==source, self.sources), None)
