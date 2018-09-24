#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/config/ChannelConfig.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 02.09.2018
# Last Modified Date: 02.09.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from pydantic import BaseModel
from typing import List, Dict, Any, Union
from ampel.pipeline.common.docstringutils import gendocstring
from ampel.pipeline.config.channel.StreamConfig import StreamConfig
from ampel.pipeline.config.channel.T3TaskConfig import T3TaskConfig

@gendocstring
class ChannelConfig(BaseModel):
	"""
	Config holder for AMPEL channels
	"""
	channel: str
	active: bool = True
	author: str = "Unspecified"
	sources: List[StreamConfig]
	t3Supervise: Union[None, T3TaskConfig, List[T3TaskConfig]] = None

	def get_stream_config(self, source):
		return next(filter(lambda x: x.stream==source, self.sources), None)
