#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/config/channel/T0UnitConfig.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 02.09.2018
# Last Modified Date: 17.09.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Dict, Any, Union
from pydantic import BaseModel
from ampel.pipeline.common.docstringutils import gendocstring

@gendocstring
class T0UnitConfig(BaseModel):
	"""
	Config holder for T0 units (filters)
	"""
	unitId: str
	runConfig: Union[None, Dict[str, Any]] = None
