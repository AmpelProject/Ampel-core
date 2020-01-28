#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/model/operator/AllOf.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 15.10.2018
# Last Modified Date: 10.10.2019
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from ampel.model.AmpelBaseModel import AmpelBaseModel
from typing import List, Union

class AllOf(AmpelBaseModel):
	allOf: Union[List[int], List[str]]
