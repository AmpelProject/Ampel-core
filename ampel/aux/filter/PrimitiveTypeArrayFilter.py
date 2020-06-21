#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/aux/filter/PrimitiveTypeArrayFilter.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 14.01.2020
# Last Modified Date: 18.06.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import List, Sequence, TypeVar
from ampel.model.aux.FilterCriterion import FilterCriterion
from ampel.aux.filter.AbsLogicOperatorFilter import AbsLogicOperatorFilter

T = TypeVar("T", int, str, float, str)

class PrimitiveTypeArrayFilter(AbsLogicOperatorFilter[T]):
	"""
	In []: f = PrimitiveTypeArrayFilter(filters={'operator': '>', 'value': 2})
	In []: f.apply([1, 2, 3, 4])
	Out[]: [3, 4]

	In []: f = PrimitiveTypeArrayFilter(filters={
		'all_of': [
			{'operator': '>', 'value': 2},
			{'operator': '<', 'value': 4}
		]
	})
	In []: f.apply([1, 2, 3, 4])
	Out[]: [3]
	"""

	@staticmethod
	def _apply_filter(args: Sequence[T], f: FilterCriterion) -> List[T]:
		return [s for s in args if f.operator(s, f.value)]
