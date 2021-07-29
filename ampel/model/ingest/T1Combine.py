#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/model/ingest/T1Combine.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 10.03.2020
# Last Modified Date: 27.05.2021
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Optional, Union, List, Sequence
from ampel.model.UnitModel import UnitModel
from ampel.model.ingest.T2Compute import T2Compute

class T1Combine(UnitModel):
	"""
	Combine datapoints into t1 document and create associated t2 documents
	"""

	#: Filter result codes that should trigger this T1.
	#: If not specified, T1 documents will be created in response to any passing alert.
	group: Optional[Union[int, List[int]]]

	#: Create or update :class:`T2 documents <ampel.content.T2Document.T2Document>`
	#: bound to :class:`compounds <ampel.content.T1Document.T1Document>`
	state_t2: Optional[Sequence[T2Compute]] = []

	#: Create or update :class:`T2 documents <ampel.content.T2Document.T2Document>`
	#: bound to :class:`datapoints <ampel.content.DataPoint.DataPoint>`
	#: based on the t1 combine unit result
	point_t2: Optional[Sequence[T2Compute]]
