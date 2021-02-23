#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/model/ingest/T2IngestModel.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 10.03.2020
# Last Modified Date: 19.11.2020
# Last Modified By  : Jakob van Santen <jakob.van.santen@desy.de>

from typing import Any, Dict, List, Union, Optional, Type
from ampel.model.StrictModel import StrictModel
from ampel.abstract.AbsStockT2Unit import AbsStockT2Unit
from ampel.abstract.AbsPointT2Unit import AbsPointT2Unit
from ampel.abstract.AbsStateT2Unit import AbsStateT2Unit
from ampel.abstract.AbsCustomStateT2Unit import AbsCustomStateT2Unit

class T2IngestModel(StrictModel):

	#: T2 unit to run
	unit: Union[str, Type[AbsStockT2Unit], Type[AbsPointT2Unit], Type[AbsStateT2Unit], Type[AbsCustomStateT2Unit]]
	#: T2 unit configuration (hashed)
	config: Optional[int]
	#: Ingester options
	ingest: Optional[Dict[str, Any]]
	#: Filter result codes that should trigger this T2. If not specified, T2
	#: documents will be created in response to *any* passing alert.
	group: Union[int, List[int]] = []

	@property
	def unit_id(self) -> str:
		if isinstance(self.unit, str):
			return self.unit
		else:
			return self.unit.__name__
