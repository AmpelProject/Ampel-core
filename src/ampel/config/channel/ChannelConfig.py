#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/config/channel/ChannelConfig.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 02.09.2018
# Last Modified Date: 05.10.2019
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import json, pkg_resources
from pydantic import BaseModel, validator
from typing import List, Union, Tuple
from ampel.common.docstringutils import gendocstring
from ampel.config.AmpelBaseModel import AmpelBaseModel
from ampel.config.channel.StreamConfig import StreamConfig
from ampel.config.ReadOnlyDict import ReadOnlyDict

@gendocstring
class ChannelConfig(AmpelBaseModel):
	"""
	Config holder for AMPEL channels

	Note: pydantic does not support typing.Sequence
	https://github.com/samuelcolvin/pydantic/issues/185
	We are thus forced to use Union[List, Tuple]
	"""
	name: Union[int, str]
	hash: int = None
	active: bool = True
	author: str = "Unspecified"
	sources: Union[List[StreamConfig], Tuple[StreamConfig]]


	@validator('sources', pre=True, whole=True)
	def validate_source(cls, sources):
		""" """
		# cast to tuple if dict
		if isinstance(sources, dict):
			return (sources, )

		return sources


	def get_stream_config(self, stream_name: str) -> Union[None, StreamConfig]:
		""" """
		return next(
			filter(
				lambda x: x.stream == stream_name, 
				self.sources
			), 
			None
		)
