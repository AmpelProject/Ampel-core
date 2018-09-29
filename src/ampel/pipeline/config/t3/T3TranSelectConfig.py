#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/config/t3/T3TranSelectConfig.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 29.09.2018
# Last Modified Date: 29.09.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from pydantic import BaseModel
from typing import Union, List
from ampel.pipeline.config.time.TimeConstraintConfig import TimeConstraintConfig
from ampel.pipeline.common.docstringutils import gendocstring

@gendocstring
class T3TranSelectConfig(BaseModel):
	""" """
	created: TimeConstraintConfig
	modified: TimeConstraintConfig
	channels: Union[str, List[str]]
	withoutFlags: Union[str, List[str]]
