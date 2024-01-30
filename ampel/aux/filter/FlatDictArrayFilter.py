#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/aux/filter/FlatDictArrayFilter.py
# License:             BSD-3-Clause
# Author:              valery brinnel <firstname.lastname@gmail.com>
# Date:                14.01.2020
# Last Modified Date:  18.06.2020
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

from collections.abc import Mapping, MutableMapping, Sequence

from ampel.aux.filter.AbsLogicOperatorFilter import AbsLogicOperatorFilter
from ampel.aux.filter.SimpleDictArrayFilter import SimpleDictArrayFilter
from ampel.model.aux.FilterCriterion import FilterCriterion
from ampel.util.mappings import flatten_dict, unflatten_dict


class FlatDictArrayFilter(AbsLogicOperatorFilter[MutableMapping]):
	"""
	In []: f = FlatDictArrayFilter(filters={'attribute': 'a.b', 'operator': '>', 'value': 2})
	In []: f.apply([{'a': {'b': 1}}, {'a': {'b':4}}])
	Out[]: [{'a': {'b': 4}}]
	"""

	@staticmethod
	def _apply_filter(dicts: Sequence[Mapping], f: FilterCriterion) -> list[MutableMapping]:
		return [
			unflatten_dict(ell)
			for ell in SimpleDictArrayFilter._apply_filter([flatten_dict(el) for el in dicts], f)  # noqa: SLF001
		]
