#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/model/t3/LoaderDirective.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 09.12.2019
# Last Modified Date: 18.06.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Dict, Any, Optional, Type, Literal
from ampel.content.StockDocument import StockDocument
from ampel.content.DataPoint import DataPoint
from ampel.content.Compound import Compound
from ampel.content.T2Document import T2Document
from ampel.content.LogDocument import LogDocument
from ampel.model.StrictModel import StrictModel

models = {
	"stock": StockDocument,
	"t0": DataPoint,
	"t1": Compound,
	"t2": T2Document,
	"log": LogDocument
}

class LoaderDirective(StrictModel):
	"""Specification of documents to load"""

	#: Source collection
	col: Literal["stock", "t0", "t1", "t2", "log"]
	model: Optional[Type] # TypedDict
	#: Mongo match expression to include in the query
	query_complement: Optional[Dict[str, Any]]
	# key "link_config" used in DBContentLoader
	options: Optional[Dict[str, Any]]

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
