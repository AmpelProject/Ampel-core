#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/model/ingest/T1CombineComputeNow.py
# License:             BSD-3-Clause
# Author:              valery brinnel <firstname.lastname@gmail.com>
# Date:                10.03.2020
# Last Modified Date:  27.05.2021
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

from collections.abc import Sequence
from ampel.model.UnitModel import UnitModel
from ampel.model.ingest.T2Compute import T2Compute

class T1CombineComputeNow(UnitModel):
	"""
	Combine datapoints into a t1 document requiring computation by a AbsT1ComputeUnit,
	which is run on the fly. Then, create associated t2 documents
	"""

	#: Filter result codes that should trigger this T1.
	#: If not specified, T1 documents will be created in response to any passing alert.
	group: None | int | Sequence[int] = None

	#: T1 compute unit to run on the fly
	compute: UnitModel

	#: Create or update :class:`T2 documents <ampel.content.T2Document.T2Document>`
	#: bound to :class:`compounds <ampel.content.T1Document.T1Document>`
	state_t2: None | Sequence[T2Compute] = []

	#: Create or update :class:`T2 documents <ampel.content.T2Document.T2Document>`
	#: bound to :class:`datapoints <ampel.content.DataPoint.DataPoint>`
	#: based on the t1 combine unit result
	point_t2: None | Sequence[T2Compute] = None
