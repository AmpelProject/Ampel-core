#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/model/t3/LoaderDirective.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 09.12.2019
# Last Modified Date: 18.06.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Dict, Any, Optional, Type, Literal
from ampel.content.StockRecord import StockRecord
from ampel.content.DataPoint import DataPoint
from ampel.content.Compound import Compound
from ampel.content.T2Record import T2Record
from ampel.content.LogRecord import LogRecord
from ampel.model.StrictModel import StrictModel

models = {
	"stock": StockRecord,
	"t0": DataPoint,
	"t1": Compound,
	"t2": T2Record,
	"log": LogRecord
}

class LoaderDirective(StrictModel):

	col: Literal["stock", "t0", "t1", "t2", "log"]
	model: Optional[Type] # TypedDict
	query_complement: Optional[Dict[str, Any]]
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
