#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/model/ingest/T1CombineCompute.py
# License:             BSD-3-Clause
# Author:              valery brinnel <firstname.lastname@gmail.com>
# Date:                27.05.2021
# Last Modified Date:  27.05.2021
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

from collections.abc import Sequence
from ampel.model.UnitModel import UnitModel

class T1CombineCompute(UnitModel):
	"""
	Combine datapoints and request computation for the created t1 document
	"""

	#: Filter result codes that should trigger this T1.
	#: If not specified, T1 documents will be created in response to any passing alert.
	group: None | int | Sequence[int] = None

	#: T1 compute unit
	compute: UnitModel
