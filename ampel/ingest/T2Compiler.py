#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/ingest/T2Compiler.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 01.01.2018
# Last Modified Date: 21.11.2021
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Optional, Dict, Union, Tuple, Set, Any, List, FrozenSet
from ampel.types import ChannelId, UnitId, T2Link, StockId
from ampel.content.T2Document import T2Document
from ampel.content.MetaActivity import MetaActivity
from ampel.abstract.AbsDocIngester import AbsDocIngester
from ampel.abstract.AbsCompiler import AbsCompiler, ActivityRegister


class T2Compiler(AbsCompiler):
	"""
	Helps build a minimal set of :class:`T2Document <ampel.content.T2Document.T2Document>`
	Multiple T2 documents associated with the same stock record accross different channels
	are merged into one single T2 document that references all corresponding channels.
	"""

	def __init__(self, col: Optional[str] = None, **kwargs) -> None:

		super().__init__(**kwargs)
		self.col = col
		self.t2s: Dict[
			# key: (unit name, unit config, link, stock)
			Tuple[UnitId, Optional[int], T2Link, StockId],
			Tuple[
				Set[ChannelId], # channels (doc)
				Dict[
					FrozenSet[Tuple[str, Any]], # key: traceid
					Tuple[ActivityRegister, Dict[str, Any]] # activity register, meta_extra
				]
			]
		] = {}


	def add(self, # type: ignore[override]
		unit: UnitId,
		config: Optional[int],
		stock: StockId,
		link: T2Link,
		channel: ChannelId,
		traceid: Dict[str, Any],
		activity: Optional[Union[MetaActivity, List[MetaActivity]]] = None,
		meta_extra: Optional[Dict[str, Any]] = None
	) -> None:
		"""
		:param tag: tag(s) to be added to T2Document. A corresponding dedicated channel-less
		meta entry will be created (with actin MetaActionCode.ADD_INGEST_TAG and/or ADD_OTHER_TAG)
		"""

		# Doc id
		k = (unit, config, link, stock)
		tid = frozenset(traceid.items())

		# a: tuple({channels}, dict[traceid, (activity register, meta_extra)])
		if a := self.t2s.get(k):

			a[0].add(channel) # set of all channels

			# One meta record will be created for each unique trace id
			# (the present compiler could be used by multiple handlers configured differently)
			if tid in a[1]:

				# (activity register, meta_extra)
				b = a[1][tid]

				# update internal register
				self.register_meta_info(b[0], b[1], channel, activity, meta_extra)

			else:
				a[1][tid] = self.new_meta_info(channel, activity, meta_extra)
		else:
			self.t2s[k] = {channel}, {tid: self.new_meta_info(channel, activity, meta_extra)}


	def commit(self, ingester: AbsDocIngester[T2Document], now: Union[int, float], **kwargs) -> None:

		for k, v in self.t2s.items():

			# Note: mongodb maintains key order
			d: T2Document = {'unit': k[0], 'config': k[1], 'link': k[2]}

			if self.col:
				d['col'] = self.col

			d['stock'] = k[3]

			if self.origin:
				d['origin'] = self.origin

			d['channel'] = list(v[0])
			d['meta'], tags = self.build_meta(v[1], now)

			if tags:
				d['tag'] = tags

			ingester.ingest(d)

		self.t2s.clear()
