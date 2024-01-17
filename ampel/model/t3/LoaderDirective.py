#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/model/t3/LoaderDirective.py
# License:             BSD-3-Clause
# Author:              valery brinnel <firstname.lastname@gmail.com>
# Date:                09.12.2019
# Last Modified Date:  02.12.2021
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

from typing import Any, Literal

from ampel.content.DataPoint import DataPoint
from ampel.content.StockDocument import StockDocument
from ampel.content.T1Document import T1Document
from ampel.content.T2Document import T2Document
from ampel.model.t3.AliasableModel import AliasableModel

models = {
	"stock": StockDocument,
	"t0": DataPoint,
	"t1": T1Document,
	"t2": T2Document,
}

class LoaderDirective(AliasableModel):
	"""Specification of documents to load"""

	#: Source collection
	col: Literal["stock", "t0", "t1", "t2"]

	#: Mongo match expression to include in the query
	query_complement: None | dict[str, Any]

	#: whether to replace init config integer hash with 'resolved' config dict
	resolve_config: bool = False

	#: whether an emtpy find() result should discard entirely the associated stock for further processing
	excluding_query: bool = False

	@property
	def model(self) -> type:
		return models[self.col]
