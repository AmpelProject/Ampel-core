#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/model/aux/LogicOperatorFilterModel.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 14.01.2020
# Last Modified Date: 17.06.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import collections, operator
from pydantic import validator
from typing import Dict, Any, Callable, Type, Optional
from ampel.model.StrictModel import StrictModel

ops: Dict[str, Callable[[str, Any], bool]] = {
	'>': operator.gt,
	'<': operator.lt,
	'>=': operator.ge,
	'<=': operator.le,
	'==': operator.eq,
	'!=': operator.ne,
	'contains': operator.contains,
	'is': operator.is_,
	'is not': operator.is_not
}

class FilterCriterion(StrictModel):

	attribute: Optional[str] = None
	type: Optional[Type] = None
	operator: Callable
	value: Any

	@validator('type', pre=True)
	def load_type(cls, v):
		if isinstance(v, str):
			return getattr(collections.abc, v)
		return v

	@validator('operator', pre=True)
	def load_operator(cls, v):
		if isinstance(v, str):
			if v not in ops:
				raise ValueError(f"Unknown operator: {v}")
			return ops[v]
		return v
