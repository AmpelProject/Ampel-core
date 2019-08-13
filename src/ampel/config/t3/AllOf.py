#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/config/t3/AllOf.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 15.10.2018
# Last Modified Date: 15.10.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from pydantic import BaseModel
from typing import List, Union

class AllOf(BaseModel):
	allOf: Union[List[int], List[str]]
