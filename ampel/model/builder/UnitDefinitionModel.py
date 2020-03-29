#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/model/builder/UnitDefinitionModel.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 06.11.2019
# Last Modified Date: 06.11.2019
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import List
from ampel.model.AmpelStrictModel import AmpelStrictModel

class UnitDefinitionModel(AmpelStrictModel):
	"""
	"""
	class_name: str
	short_mro: List['str']
