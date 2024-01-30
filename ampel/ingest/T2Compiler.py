#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/ingest/T2Compiler.py
# License:             BSD-3-Clause
# Author:              valery brinnel <firstname.lastname@gmail.com>
# Date:                01.01.2018
# Last Modified Date:  21.11.2021
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

import datetime
from typing import Any, NamedTuple

from ampel.abstract.AbsCompiler import AbsCompiler, ActivityRegister
from ampel.abstract.AbsDocIngester import AbsDocIngester
from ampel.content.MetaActivity import MetaActivity
from ampel.content.T2Document import T2Document
from ampel.types import ChannelId, StockId, T2Link, UnitId


class T2Compiler(AbsCompiler):
	"""
	Helps build a minimal set of :class:`T2Document <ampel.content.T2Document.T2Document>`
	Multiple T2 documents associated with the same stock record accross different channels
	are merged into one single T2 document that references all corresponding channels.
	"""

	class UnitKey(NamedTuple):
		unit: UnitId
		config: None | int
		link: T2Link
		stock: StockId
	
	class DocInfo(NamedTuple):
		channels: set[ChannelId]
		ttl: None | datetime.timedelta
		meta: dict[
			frozenset[tuple[str, Any]], # key: traceid
			tuple[
				ActivityRegister,
				dict[str, Any]
			]
		]		

	col: None | str = None

	def __init__(self, **kwargs) -> None:
		super().__init__(**kwargs)
		self.t2s: dict[T2Compiler.UnitKey, T2Compiler.DocInfo] = {}

	def add(self, # type: ignore[override]
		unit: UnitId,
		config: None | int,
		stock: StockId,
		link: T2Link,
		channel: ChannelId,
		ttl: None | datetime.timedelta,
		traceid: dict[str, Any],
		activity: None | MetaActivity | list[MetaActivity] = None,
		meta_extra: None | dict[str, Any] = None
	) -> None:
		"""
		:param tag: tag(s) to be added to T2Document. A corresponding dedicated channel-less
		meta entry will be created (with actin MetaActionCode.ADD_INGEST_TAG and/or ADD_OTHER_TAG)
		"""

		# Doc id
		k = T2Compiler.UnitKey(unit, config, link, stock)
		tid = frozenset(traceid.items())

		# a: tuple({channels}, dict[traceid, (activity register, meta_extra)])
		if (a := self.t2s.get(k)) is not None:

			a.channels.add(channel) # set of all channels

			# One meta record will be created for each unique trace id
			# (the present compiler could be used by multiple handlers configured differently)
			if tid in a.meta:

				# (activity register, meta_extra)
				b = a.meta[tid]

				# update internal register
				self.register_meta_info(b[0], b[1], channel, activity, meta_extra)

			else:
				a.meta[tid] = self.new_meta_info(channel, activity, meta_extra)

			if ttl is not None and (a.ttl is None or ttl > a.ttl):
				self.t2s[k] = T2Compiler.DocInfo(a.channels, ttl, a.meta)
		else:
			self.t2s[k] = T2Compiler.DocInfo({channel}, ttl, {tid: self.new_meta_info(channel, activity, meta_extra)})


	def commit(self, ingester: AbsDocIngester[T2Document], now: int | float, **kwargs) -> None:

		for k, v in self.t2s.items():

			# Note: mongodb maintains key order
			d: T2Document = {'unit': k.unit, 'config': k.config, 'link': k.link} # type: ignore[typeddict-item]

			if self.col:
				d['col'] = self.col

			d['stock'] = k.stock

			if self.origin:
				d['origin'] = self.origin

			d['channel'] = list(v.channels)
			d['meta'], tags = self.build_meta(v.meta, now)
			if v.ttl is not None:
				d['expiry'] = datetime.datetime.fromtimestamp(
					now, tz=datetime.timezone.utc
				) + v.ttl

			if tags:
				d['tag'] = tags

			ingester.ingest(d)

		self.t2s.clear()
