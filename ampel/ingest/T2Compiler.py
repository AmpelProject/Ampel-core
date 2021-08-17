#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/compile/T2Compiler.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 01.01.2018
# Last Modified Date: 12.05.2021
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from ujson import encode, decode
from typing import Optional, Dict, Union, Tuple, Set, Any
from ampel.types import ChannelId, UnitId, T2Link, StockId
from ampel.content.T2Document import T2Document
from ampel.abstract.AbsDocIngester import AbsDocIngester
from ampel.abstract.AbsCompiler import AbsCompiler
from ampel.util.collections import try_reduce


class T2Compiler(AbsCompiler):
	"""
	Helps build a minimal set of :class:`compounds <ampel.content.T2Document.T2Document>`
	Multiple T2 documents associated with the same stock record accross different channels
	are merged into one single T2 document that references all corresponding channels.
	"""

	def __init__(self, col: Optional[str] = None, **kwargs) -> None:
		super().__init__(**kwargs)
		self.t2s: Dict[
			# key: (unit name, unit config, link, stock)
			Tuple[UnitId, Optional[int], T2Link, StockId],
			# tuple[channels, dict[serialized(meta)|None, {channels}]]
			Tuple[Set[ChannelId], Dict[Optional[str], Set[ChannelId]]]
		] = {}
		self.col = col


	def add(self, # type: ignore[override]
		unit: UnitId,
		config: Optional[int],
		stock: StockId,
		link: T2Link,
		channel: ChannelId,
		meta: Optional[Dict[str, Any]] = None
	) -> None:

		# unprocessed T1 documents requiring processing have stock equals zero
		if stock == 0:
			return

		k = (unit, config, link, stock)
		k2 = encode(meta, sort_keys=True) if meta else None
		
		d = self.t2s.get(k)
		if d:
			d[0].add(channel)
			if k2 in d[1]:
				d[1][k2].add(channel)
			else:
				d[1][k2] = {channel}
		else:
			self.t2s[k] = {channel}, {k2: {channel}}


	def commit(self, ingester: AbsDocIngester[T2Document], now: Union[int, float]) -> None:

		for k, v in self.t2s.items():

			# Note: mongodb maintains key order
			d: T2Document = {'unit': k[0], 'config': k[1], 'link': k[2]}

			if self.col:
				d['col'] = self.col

			d['stock'] = k[3]

			if self.origin:
				d['origin'] = self.origin

			d['channel'] = list(v[0])

			if self._tag:
				d['tag'] = self._tag

			d['meta'] = []
			for k2, v2 in v[1].items():

				entry = {
					'run': self.run_id,
					'ts': now,
					'tier': self.tier,
					'channel': try_reduce(list(v2))
				}

				if k2 and (x := decode(k2)):
					entry |= x

				d['meta'].append(entry) # type: ignore[attr-defined]

			ingester.ingest(d, now)

		self.t2s.clear()
