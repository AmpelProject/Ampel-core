#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/model/ingest/FilterModel.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 14.10.2019
# Last Modified Date: 20.05.2021
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Optional, Dict, Literal
from ampel.model.UnitModel import UnitModel


class FilterModel(UnitModel):

	#: How to store rejection records
	reject: Optional[Dict[Literal['log', 'register'], UnitModel]]

	#: How to handle the defined filter if the associated stock is already in the database
	on_stock_match: Optional[Literal['bypass', 'overrule', 'silent_overrule']]
