#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/config/t3/T3TranConfig.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 30.09.2018
# Last Modified Date: 30.09.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from pydantic import BaseModel
from ampel.pipeline.common.docstringutils import gendocstring
from ampel.pipeline.config.t3.T3TranSelectConfig import T3TranSelectConfig
from ampel.pipeline.config.t3.T3TranLoadConfig import T3TranLoadConfig

@gendocstring
class T3TranConfig(BaseModel):
	""" """
	select: T3TranSelectConfig 
	load: T3TranLoadConfig
	chunk: int = 200
