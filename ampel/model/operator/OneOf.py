#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/model/operator/OneOf.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 22.10.2018
# Last Modified Date: 10.10.2019
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import List, Union
from ampel.model.AmpelBaseModel import AmpelBaseModel

class OneOf(AmpelBaseModel):
	oneOf: Union[List[int], List[str]]
