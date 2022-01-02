#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/model/ingest/T2Compute.py
# License:             BSD-3-Clause
# Author:              valery brinnel <firstname.lastname@gmail.com>
# Date:                10.03.2020
# Last Modified Date:  28.09.2021
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

from typing import Generic
from collections.abc import Sequence
from ampel.types import T
from ampel.model.UnitModel import UnitModel
from ampel.model.DPSelection import DPSelection

class T2Compute(UnitModel[T], Generic[T]):

	#: Ingester options
	ingest: None | str | DPSelection = None

	#: Filter result codes that should trigger this T2. If not specified, T2
	#: documents will be created in response to any passing alert.
	group: int | Sequence[int] = []
