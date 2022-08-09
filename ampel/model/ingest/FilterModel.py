#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/model/ingest/FilterModel.py
# License:             BSD-3-Clause
# Author:              valery brinnel <firstname.lastname@gmail.com>
# Date:                14.10.2019
# Last Modified Date:  20.05.2021
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

from typing import Literal
from ampel.model.UnitModel import UnitModel


class FilterModel(UnitModel[str]):

	#: How to store rejection records
	reject: None | dict[Literal['log', 'register'], UnitModel] = None

	#: How to handle the defined filter if the associated stock is already in the database
	on_stock_match: None | Literal['bypass', 'overrule', 'silent_overrule'] = None
