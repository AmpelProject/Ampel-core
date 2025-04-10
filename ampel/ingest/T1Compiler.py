#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/ingest/T1Compiler.py
# License:             BSD-3-Clause
# Author:              valery brinnel <firstname.lastname@gmail.com>
# Date:                01.01.2018
# Last Modified Date:  21.11.2021
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

import datetime
from collections.abc import Sequence
from struct import pack
from typing import Any, NamedTuple

import xxhash

from ampel.abstract.AbsCompiler import AbsCompiler, ActivityRegister
from ampel.content.MetaActivity import MetaActivity
from ampel.content.T1Document import T1Document
from ampel.protocol.DocIngesterProtocol import DocIngesterProtocol
from ampel.types import ChannelId, DataPointId, StockId, UBson, UnitId


class T1Compiler(AbsCompiler):
	"""
	Helps build a minimal set of :class:`T1Document <ampel.content.T1Document.T1Document>`
	that represent a collection of :class:`datapoints <ampel.content.DataPoint.DataPoint>`,
	as viewed through a set of channels.

	Different T1 units may select different subsets of datapoints associated with a stock.
	Only one :class:`~ampel.content.T1Document.T1Document` will be created for each
	effective subselection. This allows downstream calculations that operate on equivalent
	subselections to be performed only once.

	Excluded datapoints may be included as metadata in T1Document.meta.excl
	"""

	#: true: datapoints will have the same hash regardless of their order (unordered set)
	#: false: order of datapoints will influence the computed hash
	sort: bool = True

	#: Convention: negative size means the int hash result will be converted to signed int
	#: (necessary when using mongodb)
	#: Use 64/-64 if you expect more than 10K states per stock(s)
	hash_size: int = -32

	#: Change this only if you know what you're doing
	seed: int = 0

	class UnitKey(NamedTuple):
		unit: None | UnitId
		config: None | int
		stock: StockId
		dps: tuple[DataPointId, ...]

	class LinkTarget(NamedTuple):
		link: int
		channels: set[ChannelId]
		ttl: None | datetime.timedelta
		body: UBson
		code: None | int
		meta: dict[
			frozenset[tuple[str, Any]], # key: traceid
			tuple[
				ActivityRegister,
				dict[str, Any]  # meta extra
			]
		]


	def __init__(self, **kwargs) -> None:

		super().__init__(**kwargs)
		self.hasher = getattr(xxhash, f'xxh{abs(self.hash_size)}_intdigest')

		# Internal structure used for compiling documents
		self.t1s: dict[T1Compiler.UnitKey, T1Compiler.LinkTarget] = {}


	def add(self, # type: ignore[override]
		dps: Sequence[DataPointId],
		channel: ChannelId,
		ttl: None | datetime.timedelta,
		traceid: dict[str, Any],
		stock: StockId = 0,
		activity: None | MetaActivity | list[MetaActivity] = None,
		meta_extra: None | dict[str, Any] = None,
		unit: None | UnitId = None,
		config: None | int = None,
		body: UBson = None,
		code: None | int = None
	) -> int:
		"""
		:param unit: potential AbsT1ComputeUnit subclass to be associated with this doc
		:param config: config of the AbsT1ComputeUnit subclass associated with this doc
		"""

		if not dps:
			raise ValueError("Argument dps cannot be empty")

		if self.sort:
			# Note: int DataPointId is implicitely required,
			# hashing of single dp ids will be required if bytes is used rather than int
			dps = sorted(dps)

		k = T1Compiler.UnitKey(unit, config, stock, tuple(dps))
		tid = frozenset(traceid.items())

		# a: (link, {channels}, body, code, dict[traceid, (ActivityRegister, meta extra)])
		# a: ( 0  ,     1     ,  2  ,  3  ,                   4                        )])
		if a := self.t1s.get(k):

			a.channels.add(channel)

			# One meta record will be created for each unique trace id
			# (the present compiler could be used by multiple handlers configured differently)
			# activity can contain channel-based dps exclusions (excl: [1232, 3232])
			if tid in a.meta:

				# (activity register, meta_extra)
				b = a.meta[tid]

				# update internal register
				self.register_meta_info(b[0], b[1], channel, activity, meta_extra)
			else:
				a.meta[tid] = self.new_meta_info(channel, activity, meta_extra)

			# Update ttl if necessary
			if ttl is not None and (a.ttl is None or ttl > a.ttl):
				self.t1s[k] = T1Compiler.LinkTarget(a.link, a.channels, ttl, a.body, a.code, a.meta)

		else:

			i = self.hasher(
				pack(f">{len(dps)}q", *dps),
				self.seed
			)

			# Convert unsigned to signed
			if self.hash_size < 0 and i & (1 << (-self.hash_size-1)):
				i -= 2**-self.hash_size

			a = self.t1s[k] = T1Compiler.LinkTarget(
				i, {channel}, ttl, body, code,
				{tid: self.new_meta_info(channel, activity, meta_extra)}
			)

		return a.link


	def commit(self, ingester: DocIngesterProtocol[T1Document], now: int | float, **kwargs) -> None:

		# t1: (unit, config, stock, dps)
		# t2: (link, {channels}, body, code, dict[traceid, (ActivityRegister, meta extra)])
		# t2: ( 0  ,     1     ,  2  ,  3  ,                   4                        )])
		for t1, t2 in self.t1s.items():

			# Note: mongodb maintains key order
			d: T1Document = {'link': t2.link} # type: ignore[typeddict-item]

			if t1.unit:
				d['unit'] = t1.unit

			if t1.config is not None:
				d['config'] = t1.config

			d['stock'] = t1.stock

			if self.origin:
				d['origin'] = self.origin

			d['channel'] = list(t2.channels)
			d['dps'] = list(t1.dps)

			d['meta'], tags = self.build_meta(t2.meta, now)

			if tags:
				d['tag'] = tags

			if t2.body is not None:
				d['body'] = [t2.body]

			if t2.code is not None:
				d['code'] = t2.code

			if t2.ttl is not None:
				d['expiry'] = datetime.datetime.fromtimestamp(
					now, tz=datetime.timezone.utc
				) + t2.ttl

			ingester.ingest(d)

		self.t1s.clear()
