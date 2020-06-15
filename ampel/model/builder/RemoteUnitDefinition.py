#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/model/builder/RemoteUnitDefinition.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 06.11.2019
# Last Modified Date: 15.06.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import List
from ampel.model.StrictModel import StrictModel

class RemoteUnitDefinition(StrictModel):

	class_name: str
	base: List['str']
