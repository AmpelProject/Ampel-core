#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/aux/filter/AbsLogicOperatorFilter.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 14.01.2020
# Last Modified Date: 18.06.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Union, Generic, List, Sequence, Any
from ampel.types import T
from ampel.base.decorator import abstractmethod
from ampel.model.operator.AllOf import AllOf
from ampel.model.operator.FlatAnyOf import FlatAnyOf
from ampel.model.aux.FilterCriterion import FilterCriterion
from ampel.abstract.AbsApplicable import AbsApplicable


class AbsLogicOperatorFilter(Generic[T], AbsApplicable, abstract=True):

	filters: Union[
		FilterCriterion,
		FlatAnyOf[FilterCriterion],
		AllOf[FilterCriterion]
	]

	@staticmethod
	@abstractmethod
	def _apply_filter(args: Sequence[T], f: FilterCriterion) -> List[T]:
		...

	def apply(self, args: Sequence[T]) -> List[T]:

		if isinstance(self.filters, FilterCriterion):
			return self._apply_filter(args, self.filters)

		if isinstance(self.filters, AllOf):
			ret: Any = args
			for f in self.filters.all_of:
				ret = self._apply_filter(ret, f)
			return ret

		if isinstance(self.filters, FlatAnyOf):
			ret = []
			for f in self.filters.any_of:
				for el in self._apply_filter(args, f):
					if el not in ret:
						ret.append(el)
			return ret

		raise ValueError("Incorrect type provided for parameter 'filters'")
