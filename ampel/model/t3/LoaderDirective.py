#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/model/t3/LoaderDirective.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 09.12.2019
# Last Modified Date: 16.02.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from pydantic import validator, root_validator
from typing import Dict, Any, Optional, Type, Literal
from ampel.content.StockRecord import StockRecord
from ampel.content.DataPoint import DataPoint
from ampel.content.Compound import Compound
from ampel.content.T2Record import T2Record
from ampel.content.LogRecord import LogRecord
from ampel.model.AmpelStrictModel import AmpelStrictModel

models = {
	"stock": StockRecord,
	"t0": DataPoint,
	"t1": Compound,
	"t2": T2Record,
	"logs": LogRecord
}

class LoaderDirective(AmpelStrictModel):
	""" """

	col: Literal["stock", "t0", "t1", "t2", "logs"]
	model: Optional[Type] # TypedDict
	query_complement: Optional[Dict[str, Any]]
	options: Optional[Dict[str, Any]]

	@validator('model')
	def validate(cls, v):
		if not hasattr(v, "__annotations__"):
			raise ValueError("TypedDict expected for parameter 'model'")
		return v

	@root_validator
	def _set_defaults(cls, values):
		if not values.get('model') and values.get('col') in models:
			values['model'] = models[values.get('col')]
		return values
