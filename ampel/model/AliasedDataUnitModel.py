#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/model/AliasedDataUnitModel.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 19.02.2020
# Last Modified Date: 19.02.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from ampel.model.DataUnitModel import DataUnitModel
from ampel.model.AliasedUnitModel import AliasedUnitModel

class AliasedDataUnitModel(AliasedUnitModel, DataUnitModel):
	""" Note: inheritance order matters """
	pass
