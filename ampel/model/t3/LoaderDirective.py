#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/model/t3/LoaderDirective.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 09.12.2019
# Last Modified Date: 02.12.2021
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Dict, Any, Optional, Type, Literal
from ampel.content.StockDocument import StockDocument
from ampel.content.DataPoint import DataPoint
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

	model: Optional[Type] # TypedDict

	#: Mongo match expression to include in the query
	query_complement: Optional[Dict[str, Any]]

	#: whether to replace init config integer hash with 'resolved' config dict
	resolve_config: bool = False

	#: whether an emtpy find() result should discard entirely the associated stock for further processing
	excluding_query: bool = False

	def __init__(self, **kwargs):
		super().__init__(**kwargs)
		if not self.model and self.col in models:
			self.model = models[self.col]
		elif self.model and not hasattr(self.model, "__annotations__"):
			raise ValueError("TypedDict expected for parameter 'model'")

	def dict(self, **kwargs):
		# do not emit model if it was equivalent to the default
		rep = super().dict(**kwargs)
		if self.model == models[self.col]:
			del rep["model"]
		return rep
