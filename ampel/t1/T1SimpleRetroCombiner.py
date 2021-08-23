#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/t1/T1SimpleRetroCombiner.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 25.05.2021
# Last Modified Date: 23.07.2021
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Iterable, List, Optional
from ampel.content.DataPoint import DataPoint
from ampel.types import DataPointId
from ampel.struct.T1CombineResult import T1CombineResult
from ampel.abstract.AbsT1CombineUnit import AbsT1CombineUnit


class T1SimpleRetroCombiner(AbsT1CombineUnit):
	"""
	[el.payload for el in combine([{'id': 7}, {'id': 6}, {'id': 5}])]
	will return:
	[[7, 6, 5], [6, 5], [5]]
	"""

	def combine(self, datapoints: Iterable[DataPoint]) -> List[T1CombineResult]: # type: ignore[override]
		"""
		:param datapoints: dict instances representing datapoints
		"""

		chan = self.channel
		dps = [
			dp['id'] for dp in datapoints
			if not("excl" in dp and chan in dp['excl'])
		]

		prev_det_sequences = [dps]
		while dps := self._prev_det_seq(dps): # type: ignore[assignment]
			prev_det_sequences.append(dps)

		return [T1CombineResult(dps=el) for el in reversed(prev_det_sequences)]


	def _prev_det_seq(self, datapoints: List[DataPointId]) -> Optional[List[DataPointId]]:
		"""
		Overridable by sub-classes (ex: T1PhotoRetroCombiner)
		"""

		if len(datapoints) > 1:
			return datapoints[:-1]
		return None
