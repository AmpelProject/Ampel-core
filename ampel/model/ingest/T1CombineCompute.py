#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/model/ingest/T1CombineCompute.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 27.05.2021
# Last Modified Date: 27.05.2021
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Optional, Union, List
from ampel.model.UnitModel import UnitModel

class T1CombineCompute(UnitModel):
	"""
	Combine datapoints and request computation for the created t1 document
	"""

	#: Filter result codes that should trigger this T1.
	#: If not specified, T1 documents will be created in response to any passing alert.
	group: Optional[Union[int, List[int]]]

	#: T1 compute unit
	compute: UnitModel
