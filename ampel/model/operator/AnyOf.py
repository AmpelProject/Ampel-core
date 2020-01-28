#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/model/operator/AnyOf.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 15.10.2018
# Last Modified Date: 10.10.2019
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Union, List
from ampel.model.operator.AllOf import AllOf
from ampel.model.AmpelBaseModel import AmpelBaseModel

class AnyOf(AmpelBaseModel):
	anyOf: List[Union[int, str, AllOf]]
