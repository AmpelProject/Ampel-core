#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/aux/filter/SimpleDictArrayFilter.py
# License:             BSD-3-Clause
# Author:              valery brinnel <firstname.lastname@gmail.com>
# Date:                14.01.2020
# Last Modified Date:  18.06.2020
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

from typing import TypeVar
from collections.abc import Mapping, Sequence
from ampel.model.aux.FilterCriterion import FilterCriterion
from ampel.aux.filter.AbsLogicOperatorFilter import AbsLogicOperatorFilter

T = TypeVar("T", bound=Mapping)


class SimpleDictArrayFilter(AbsLogicOperatorFilter[T]):
	"""
	In []: f = SimpleDictArrayFilter(filters={'attribute': 'a', 'operator': '==', 'value': 2})
	In []: f.apply([{'a': 1}, {'a': 4}])
	Out[]: []

	In []: f.apply([{'a': 1}, {'a': 2}])
	Out[]: [{'a': 2}]

	In []: f1 = SimpleDictArrayFilter(filters={'attribute': 'a', 'type': 'Sequence', 'operator': 'contains', 'value': 2})
	In []: f1.apply([{'a': [1, 2]}, {'a': 4}])
	Out[]: [{'a': [1, 2]}]

	In []: f2 = SimpleDictArrayFilter(filters={
		'any_of': [
			{'attribute': 'a', 'type': 'Sequence', 'operator': 'contains', 'value': 2},
			{'attribute': 'a', 'type': 'Sequence', 'operator': 'contains', 'value': 1}
		]
	})
	In []: f2.apply([{'a': [1, 2], 'b': {'f': 12}}, {'a': 4}, {'a': 2, 'z': 12}])
	Out[]: [{'a': [1, 2], 'b': {'f': 12}}]
	"""

	@staticmethod
	def _apply_filter(dicts: Sequence[T], f: FilterCriterion) -> list[T]:

		attr_name = f.attribute
		if f.type:
			return [
				d for d in dicts
				if attr_name in d and
				isinstance(d[attr_name], f.type) and
				f.operator(d[attr_name], f.value)
			]
		else:
			return [d for d in dicts if attr_name in d and f.operator(d[attr_name], f.value)]
