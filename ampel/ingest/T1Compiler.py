#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/ingest/T1Compiler.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 01.01.2018
# Last Modified Date: 21.11.2021
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import xxhash
from struct import pack
from typing import Optional, Dict, List, Union, Tuple, Set, Any, FrozenSet
from ampel.types import ChannelId, DataPointId, StockId, UnitId, UBson
from ampel.content.T1Document import T1Document
from ampel.content.MetaActivity import MetaActivity
from ampel.abstract.AbsDocIngester import AbsDocIngester
from ampel.abstract.AbsCompiler import AbsCompiler, ActivityRegister


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


	def __init__(self, **kwargs) -> None:

		super().__init__(**kwargs)
		self.hasher = getattr(xxhash, f'xxh{abs(self.hash_size)}_intdigest')

		# Internal structure used for compiling documents
		self.t1s: Dict[
			Tuple[
				Optional[UnitId],       # unit
				Optional[int],          # config
				StockId,                # stock
				Tuple[DataPointId, ...] # tuple(dps) [will be hashed into link]
			],
			Tuple[
				int, 	                # link
				Set[ChannelId],         # channels (doc)
				UBson,                  # body
				Optional[int],          # code
				Dict[
					FrozenSet[Tuple[str, Any]], # key: traceid
					Tuple[
						ActivityRegister,
						Dict[str, Any]  # meta extra
					]
				],
			]
		] = {}


	def add(self, # type: ignore[override]
		dps: List[DataPointId],
		channel: ChannelId,
		traceid: Dict[str, Any],
		stock: StockId = 0,
		activity: Optional[Union[MetaActivity, List[MetaActivity]]] = None,
		meta_extra: Optional[Dict[str, Any]] = None,
		unit: Optional[UnitId] = None,
		config: Optional[int] = None,
		body: UBson = None,
		code: Optional[int] = None
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

		k = (unit, config, stock, tuple(dps))
		tid = frozenset(traceid.items())

		# a: (link, {channels}, body, code, dict[traceid, (ActivityRegister, meta extra)])
		# a: ( 0  ,     1     ,  2  ,  3  ,                   4                        )])
		if a := self.t1s.get(k):

			a[1].add(channel)

			# One meta record will be created for each unique trace id
			# (the present compiler could be used by multiple handlers configured differently)
			# activity can contain channel-based dps exclusions (excl: [1232, 3232])
			if tid in a[4]:

				# (activity register, meta_extra)
				b = a[4][tid]

				# update internal register
				self.register_meta_info(b[0], b[1], channel, activity, meta_extra)
			else:
				a[4][tid] = self.new_meta_info(channel, activity, meta_extra)

		else:

			i = self.hasher(
				pack(f">{len(dps)}q", *dps),
				self.seed
			)

			# Convert unsigned to signed
			if self.hash_size < 0 and i & (1 << (-self.hash_size-1)):
				i -= 2**-self.hash_size

			a = self.t1s[k] = (
				i, {channel}, body, code,
				{tid: self.new_meta_info(channel, activity, meta_extra)}
			)

		return a[0]


	def commit(self, ingester: AbsDocIngester[T1Document], now: Union[int, float], **kwargs) -> None:

		# t1: (unit, config, stock, dps)
		# t2: (link, {channels}, body, code, dict[traceid, (ActivityRegister, meta extra)])
		# t2: ( 0  ,     1     ,  2  ,  3  ,                   4                        )])
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

			d['meta'], tags = self.build_meta(t2[4], now)

			if tags:
				d['tag'] = tags

			if t2[2]:
				d['body'] = [t2[2]]

			if t2[3] is not None:
				d['code'] = t2[3]

			ingester.ingest(d)

		self.t1s.clear()
