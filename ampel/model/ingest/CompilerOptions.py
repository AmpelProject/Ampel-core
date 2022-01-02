#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/model/ingest/CompilerOptions.py
# License:             BSD-3-Clause
# Author:              valery brinnel <firstname.lastname@gmail.com>
# Date:                08.05.2021
# Last Modified Date:  15.05.2021
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

from typing import Any
from ampel.base.AmpelBaseModel import AmpelBaseModel


empty: dict[str, Any] = {}

class CompilerOptions(AmpelBaseModel):
	"""
	Will be merged with the options set by say IngestionHandlers (these will have priority).
	Allows for example to set default tags for given documents or to define a custom AbsIdMapper
	subclass for the stock compiler.
	"""

	t0: dict[str, Any] = empty
	t1: dict[str, Any] = empty
	state_t2: dict[str, Any] = empty
	point_t2: dict[str, Any] = empty
	stock_t2: dict[str, Any] = empty
	stock: dict[str, Any] = empty
