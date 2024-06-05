#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/ingest/T0Compiler.py
# License:             BSD-3-Clause
# Author:              valery brinnel <firstname.lastname@gmail.com>
# Date:                11.05.2021
# Last Modified Date:  25.11.2021
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

import datetime
from typing import Any

from ampel.abstract.AbsCompiler import AbsCompiler
from ampel.abstract.AbsDocIngester import AbsDocIngester
from ampel.content.DataPoint import DataPoint
from ampel.content.MetaRecord import MetaRecord
from ampel.enum.MetaActionCode import MetaActionCode
from ampel.types import ChannelId, DataPointId
from ampel.util.collections import try_reduce


class T0Compiler(AbsCompiler):
	"""
	Compiles updates to t0 documents arising from different t0 directives
	"""

	def __init__(self, **kwargs) -> None:
		super().__init__(**kwargs)

		# We assume only the usage of only one shaper (that is only one tracid)
		self.register: dict[
			DataPointId,
			tuple[
				DataPoint,
				set[ChannelId],
				None | datetime.timedelta, # ttl
				None | int,           # trace id
				None | dict[str, Any] # meta extra
			]
		] = {}

		self.retained: dict[DataPointId, datetime.timedelta] = {}


	# Override
	def add(self, # type: ignore[override]
		dps: list[DataPoint],
		channel: ChannelId,
		ttl: None | datetime.timedelta,
		trace_id: None | int,
		extra: None | dict[str, Any] = None
	) -> None:

		r = self.register
		for dp in dps:
			dpid = dp['id']
			if dpid in r:
				r[dpid][1].add(channel)
				if ttl is not None and ((prev := r[dpid][2]) is None or ttl > prev):
					r[dpid] = r[dpid][:2] + (ttl,) + r[dpid][3:]
			else:
				r[dpid] = dp, {channel}, ttl, trace_id, extra


	def retain(self, dps: list[DataPoint], ttl: datetime.timedelta) -> None:
		for dp in dps:
			if dp['id'] not in self.retained or ttl > self.retained[dp['id']]:
					self.retained[dp['id']] = ttl

	# Override
	def commit(self, ingester: AbsDocIngester[DataPoint], now: int | float, **kwargs) -> None:
		"""
		Note that we let the ingester handle 'ts' and 'updated' values
		"""

		for dp, channel_sets, ttl, trace_id, extra in self.register.values():

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
				meta['activity'].append(self._ingest_tag_activity) # type: ignore[attr-defined]

			if self.origin and 'origin' not in dp:
				dp['origin'] = self.origin

			dp['channel'] = lchans
			if 'meta' in dp and isinstance(dp['meta'], list):
				dp['meta'].append(meta)
			else:
				dp['meta'] = [meta]

			if ttl is not None:
				dp['expiry'] = datetime.datetime.fromtimestamp(
					now, tz=datetime.timezone.utc
				) + ttl

			ingester.ingest(dp)

		# Retain datapoints that were not explicitly ingested
		if retained := set(self.retained).difference(self.register.keys()):
			nowdt = datetime.datetime.fromtimestamp(
				now, tz=datetime.timezone.utc
			)
			ingester.update_expiry({k: nowdt + self.retained[k] for k in retained})  # type: ignore[attr-defined]

		self.register.clear()
		self.retained.clear()
