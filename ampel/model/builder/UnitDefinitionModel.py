#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/model/builder/UnitDefinitionModel.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 06.11.2019
# Last Modified Date: 06.11.2019
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import List
from ampel.model.AmpelBaseModel import AmpelBaseModel

class UnitDefinitionModel(AmpelBaseModel):
	"""
	"""
	fqn: str
	mro: List['str']
