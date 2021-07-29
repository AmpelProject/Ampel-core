#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/model/ingest/CompilerOptions.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 08.05.2021
# Last Modified Date: 15.05.2021
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Dict, Any
from ampel.model.StrictModel import StrictModel

empty: Dict[str, Any] = {}

class CompilerOptions(StrictModel):
	"""
	Will be merged with the options set by say IngestionHandlers (these will have priority).
	Allows for example to set default tags for given documents or to define a custom AbsIdMapper
	subclass for the stock compiler.
	"""

	t0: Dict[str, Any] = empty
	t1: Dict[str, Any] = empty
	state_t2: Dict[str, Any] = empty
	point_t2: Dict[str, Any] = empty
	stock_t2: Dict[str, Any] = empty
	stock: Dict[str, Any] = empty
