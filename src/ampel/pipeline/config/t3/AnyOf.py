#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/config/t3/AnyOf.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 15.10.2018
# Last Modified Date: 15.10.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from pydantic import BaseModel
from typing import Union, List
from ampel.pipeline.config.t3.AllOf import AllOf

class AnyOf(BaseModel):
	anyOf: List[Union[str, AllOf]]
