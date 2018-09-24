#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/config/channel/StreamConfig.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 02.09.2018
# Last Modified Date: 03.09.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from pydantic import BaseModel
from typing import List, Dict, Any, Union, Optional
from ampel.pipeline.common.docstringutils import gendocstring
# no idea why pylint complains below...
# pylint: disable=E0611,E0401
from ampel.pipeline.config.channel.T0UnitConfig import T0UnitConfig
from ampel.pipeline.config.channel.T2UnitConfig import T2UnitConfig

@gendocstring
class StreamConfig(BaseModel):
	"""
	Config holder for AMPEL channel input streams (ex: ZTFIPAC) 
	-> values defined in channel configuration section 'sources'
	"""
	stream: str
	parameters: Union[None, Dict[str, Any]] = None
	t0Filter: Optional[T0UnitConfig]
	t2Compute: Optional[List[T2UnitConfig]] = None
