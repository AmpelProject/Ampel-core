#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/ingest/T0Compiler.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 11.05.2021
# Last Modified Date: 25.11.2021
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Dict, List, Optional, Set, Union, Tuple, Any
from ampel.content.MetaRecord import MetaRecord
from ampel.types import ChannelId, DataPointId
from ampel.content.DataPoint import DataPoint
from ampel.abstract.AbsDocIngester import AbsDocIngester
from ampel.util.collections import try_reduce
from ampel.abstract.AbsCompiler import AbsCompiler
from ampel.enum.MetaActionCode import MetaActionCode


class T0Compiler(AbsCompiler):
	"""
	Compiles updates to t0 documents arising from different t0 directives
	"""

	def __init__(self, **kwargs) -> None:
		super().__init__(**kwargs)

		# We assume only the usage of only one shaper (that is only one tracid)
		self.register: Dict[
			DataPointId,
			Tuple[
				DataPoint,
				Set[ChannelId],
				Optional[int],           # trace id
				Optional[Dict[str, Any]] # meta extra
			]
		] = {}


	# Override
	def add(self, # type: ignore[override]
		dps: List[DataPoint],
		channel: ChannelId,
		trace_id: Optional[int],
		extra: Optional[Dict[str, Any]] = None
	) -> None:

		r = self.register
		for dp in dps:
			dpid = dp['id']
			if dpid in r:
				r[dpid][1].add(channel)
			else:
				r[dpid] = dp, {channel}, trace_id, extra


	# Override
	def commit(self, ingester: AbsDocIngester[DataPoint], now: Union[int, float], **kwargs) -> None:
		"""
		Note that we let the ingester handle 'ts' and 'updated' values
		"""

		for dp, channel_sets, trace_id, extra in self.register.values():

			lchans = list(channel_sets)
			meta: MetaRecord = {'ts': now, 'run': self.run_id}

			if extra:
				meta.update(extra) # type: ignore[typeddict-item]

			meta['activity'] = [
				{
					'action': MetaActionCode.ADD_CHANNEL,
					'channel': try_reduce(lchans)
				}
			]
			meta['traceid'] = {'shaper': trace_id}

			if self._tag or dp.get('tag'):
				dp['tag'] = [*(self._tag or []), *(dp.get('tag') or [])]
				meta['activity'].append(self._ingest_tag_activity) # type: ignore

			if self.origin and 'origin' not in dp:
				dp['origin'] = self.origin

			dp['channel'] = lchans
			if 'meta' in dp and isinstance(dp['meta'], list):
				dp['meta'].append(meta)
			else:
				dp['meta'] = [meta]

			ingester.ingest(dp)

		self.register.clear()
