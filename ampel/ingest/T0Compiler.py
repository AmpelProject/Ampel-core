#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/ingest/T0Compiler.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 11.05.2021
# Last Modified Date: 11.05.2021
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Dict, List, Set, Union, Tuple, Any
from ampel.content.MetaRecord import MetaRecord
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
		self.register: Dict[int, List[Tuple[Set[ChannelId], List[DataPoint]]]] = {}


	# Override
	def add(self, # type: ignore[override]
		dps: List[DataPoint],
		channel: ChannelId,
		trace_id: int
	) -> None:

		if trace_id not in self.register:
			self.register[trace_id] = []
		for chans, dps_to_ingest in self.register[trace_id]:
			if dps == dps_to_ingest:
				chans.add(channel)
				return
		self.register[trace_id].append(({channel}, dps))


	# Override
	def commit(self, ingester: AbsDocIngester[DataPoint], now: Union[int, float]) -> None:
		"""
		Note that we let the ingester handle 'ts' and 'updated' values
		"""

		x: Dict[DataPointId, DataPoint] = {}
		for trace_id, channel_sets in self.register.items():

			for chans, dps in channel_sets:

				for dp in dps:

					if self._tag and 'tag' not in dp:
						dp['tag'] = self._tag

					if self.origin and 'origin' not in dp:
						dp['origin'] = self.origin

					meta: MetaRecord = {
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
						# add a new point
						x[dp['id']] = dp
					else:
						# update channel set and metadata
						prev = x[dp['id']]
						prev['channel'] |= dp['channel'] # type: ignore[operator]
						if isinstance(pchan := (meta := prev['meta'][-1])['channel'], list):
							prev_chans = set(pchan)
						else:
							prev_chans = {pchan}
						meta['channel'] = try_reduce(list(prev_chans | chans))

		for dp in x.values():
			dp['channel'] = list(dp['channel'])
			ingester.ingest(dp, now)

		self.register.clear()
