#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/model/ingest/T2Compute.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 10.03.2020
# Last Modified Date: 11.05.2021
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Any, Dict, List, Union, Optional, Generic
#from ampel.model.StrictModel import StrictModel
from ampel.types import T
from ampel.model.UnitModel import UnitModel

class T2Compute(UnitModel[T], Generic[T]):

	##: T2 unit to run
	#unit: str

	#: T2 unit configuration (hashed)
	#config: Optional[int]

	#: Ingester options
	ingest: Optional[Dict[str, Any]]

	#: Filter result codes that should trigger this T2. If not specified, T2
	#: documents will be created in response to any passing alert.
	group: Union[int, List[int]] = []
