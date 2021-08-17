#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/ingest/T0Compiler.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 11.05.2021
# Last Modified Date: 11.05.2021
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Dict, List, Set, Union, Tuple, Any
from ampel.types import ChannelId, DataPointId
from ampel.content.DataPoint import DataPoint
from ampel.abstract.AbsDocIngester import AbsDocIngester
from ampel.util.collections import try_reduce
from ampel.abstract.AbsCompiler import AbsCompiler


class T0Compiler(AbsCompiler):
	"""
	Compiles updates to t0 documents arising from different t0 directives
	"""

	def __init__(self, **kwargs) -> None:
		super().__init__(**kwargs)
		self.register: Dict[int, Tuple[Set[ChannelId], List[DataPoint]]] = {}


	# Override
	def add(self, # type: ignore[override]
		dps: List[DataPoint],
		channel: ChannelId,
		trace_id: int
	) -> None:

		if trace_id in self.register:
			self.register[trace_id][0].add(channel)
		else:
			self.register[trace_id] = {channel}, dps


	# Override
	def commit(self, ingester: AbsDocIngester[DataPoint], now: Union[int, float]) -> None:
		"""
		Note that we let the ingester handle 'ts' and 'updated' values
		"""

		x: Dict[DataPointId, DataPoint] = {}
		for trace_id, (chans, dps) in self.register.items():

			for dp in dps:

				if self._tag and 'tag' not in dp:
					dp['tag'] = self._tag

				if self.origin and 'origin' not in dp:
					dp['origin'] = self.origin

				meta: Dict[str, Any] = {
					'run': self.run_id,
					'ts': now,
					'channel': try_reduce(list(chans)),
					'traceid': {'shaper': trace_id}
				}

				if 'meta' in dp:
					dp['meta'].append(meta) # type: ignore[attr-defined]
				else:
					dp['meta'] = [meta] # type: ignore[list-item]

				if 'channel' in dp:
					if isinstance(dp['channel'], list):
						dp['channel'] = chans | set(dp['channel']) # type: ignore[typeddict-item]

					else:
						dp['channel'] |= chans # type: ignore[operator]
				else:
					dp['channel'] = chans # type: ignore[typeddict-item]

				if dp['id'] not in x:
					x[dp['id']] = dp

		for dp in x.values():
			dp['channel'] = list(dp['channel'])
			ingester.ingest(dp, now)

		self.register.clear()
