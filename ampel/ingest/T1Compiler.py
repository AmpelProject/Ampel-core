#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/ingest/T1Compiler.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 01.01.2018
# Last Modified Date: 06.09.2021
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import xxhash
from ujson import encode, decode
from struct import pack
from typing import Optional, Dict, List, Union, Tuple, Set, Any
from ampel.types import ChannelId, DataPointId, StockId, UnitId, UBson
from ampel.content.T1Document import T1Document
from ampel.enum.MetaActionCode import MetaActionCode
from ampel.abstract.AbsDocIngester import AbsDocIngester
from ampel.abstract.AbsCompiler import AbsCompiler
from ampel.util.collections import try_reduce


class T1Compiler(AbsCompiler):
	"""
	Helps build a minimal set of :class:`compounds <ampel.content.T1Document.T1Document>`
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


	def __init__(self, **kwargs) -> None:

		super().__init__(**kwargs)
		self.hasher = getattr(xxhash, f'xxh{abs(self.hash_size)}_intdigest')

		# Internal structure used for compiling documents
		self.t1s: Dict[
			Tuple[
				Optional[UnitId],
				Optional[int], # config
				StockId,
				Tuple[DataPointId, ...] # tuple(dps)
			],
			Tuple[
				int, # link
				Set[ChannelId],
				# dict[serialized(meta)|None, {channels}]
				Dict[Optional[str], Set[ChannelId]],
				UBson, # body
				Optional[int] # code
			]
		] = {}


	def add(self, # type: ignore[override]
		dps: List[DataPointId],
		channel: ChannelId,
		stock: StockId = 0,
		meta: Optional[Dict[str, Any]] = None,
		unit: Optional[UnitId] = None,
		config: Optional[int] = None,
		body: UBson = None,
		code: Optional[int] = None
	) -> int:
		"""
		:param meta: contains excl dps and traceid -> hash(AbsT1CombineUnit + config + versions)
		:param unit: potential AbsT1ComputeUnit subclass to be associated with this doc
		:param config: config of the AbsT1ComputeUnit subclass associated with this doc
		"""

		if not dps:
			raise ValueError("Argument dps cannot be empty")

		if self.sort:
			# Note: int DataPointId is implicitely required,
			# hashing of single dp ids will be required if bytes is used rather than int
			dps = sorted(dps)

		k = (unit, config, stock, tuple(dps))
		k2 = encode(meta, sort_keys=True) if meta else None
		d = self.t1s.get(k)

		if d:
			d[1].add(channel)

			# Gather potential similar meta data (exclusion, combine trace id):
			# {run: 12, ts: ..., combine: <traceId>, excl: [1232, 3232], channel: [C1, C2, C3]}
			if k2 in d[2]:
				d[2][k2].add(channel)
			else:
				d[2][k2] = {channel}

		else:

			i = self.hasher(
				pack(f">{len(dps)}q", *dps),
				self.seed
			)

			# Convert unsigned to signed
			if self.hash_size < 0 and i & (1 << (-self.hash_size-1)):
				i -= 2**-self.hash_size

			d = self.t1s[k] = (i, {channel}, {k2: {channel}}, body, code)

		return d[0]


	def commit(self, ingester: AbsDocIngester[T1Document], now: Union[int, float]) -> None:

		# t1: tuple[unit, config, stock, dps],
		# t2: tuple[link, {channels}, dict[(trace_id, excl), {channels}]]
		for t1, t2 in self.t1s.items():

			# Note: mongodb maintains key order
			d: T1Document = {'link': t2[0]}

			if t1[0]:
				d['unit'] = t1[0] # type: ignore[typeddict-item]

			if t1[1]:
				d['config'] = t1[1] # type: ignore[typeddict-item]

			d['stock'] = t1[2]

			if self.origin:
				d['origin'] = self.origin

			d['channel'] = list(t2[1])
			d['dps'] = list(t1[3])

			if self._tag:
				d['tag'] = self._tag # type: ignore[arg-type]

			d['meta'] = []
			for k2, v2 in t2[2].items():

				entry = {
					'run': self.run_id,
					'ts': now,
					'tier': self.tier,
					'channel': try_reduce(list(v2)),
					'action': MetaActionCode.ADD_CHANNEL
				}

				if self._tag:
					entry['action'] |= MetaActionCode.ADD_TAG
					entry['tag'] = try_reduce(self._tag)

				if x := decode(k2):
					entry |= x

				d['meta'].append(entry) # type: ignore[attr-defined]

			if t2[3]:
				d['body'] = [t2[3]]

			if t2[4] is not None:
				d['code'] = t2[4]

			ingester.ingest(d, now)

		self.t1s.clear()
