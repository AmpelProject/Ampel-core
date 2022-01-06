#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/model/aux/LogicOperatorFilterModel.py
# License:             BSD-3-Clause
# Author:              valery brinnel <firstname.lastname@gmail.com>
# Date:                14.01.2020
# Last Modified Date:  17.06.2020
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

import collections, operator
from typing import Any, Type
from collections.abc import Callable
from ampel.base.AmpelBaseModel import AmpelBaseModel

ops: dict[str, Callable[[str, Any], bool]] = {
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

class FilterCriterion(AmpelBaseModel):

	attribute: None | str = None
	type: None | Type = None
	operator: Callable
	value: Any

	def __init__(self, **kwargs):
		if isinstance(type_kw := kwargs.get("type"), str):
			kwargs["type"] = getattr(collections.abc, type_kw)
		
		if isinstance(operator_kw := kwargs.get("operator"), str):
			if operator_kw not in ops:
				raise ValueError(f"Unknown operator: {operator_kw}")
			kwargs["operator"] = ops[operator_kw]

		super().__init__(**kwargs)
