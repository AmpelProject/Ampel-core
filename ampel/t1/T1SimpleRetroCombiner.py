#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/t1/T1SimpleRetroCombiner.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 25.05.2021
# Last Modified Date: 23.07.2021
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Generator, Iterable, List, Optional
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
			dp for dp in datapoints
			if not("excl" in dp and chan in dp['excl'])
		]

		return [T1CombineResult(dps=el) for el in reversed(list(self.generate_retro_sequences(dps)))]

	def generate_retro_sequences(self, datapoints: List[DataPoint]) -> Generator[List[DataPointId], None, None]:
		"""
		Generate substates by iteratively removing the last element. This may
		be overridden by subclasses, e.g. to use only certain datapionts to
		delimit states.
		"""
		while datapoints:
			yield [dp["id"] for dp in datapoints]
			datapoints = datapoints[:-1]

