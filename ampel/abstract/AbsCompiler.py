#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/abstract/AbsCompiler.py
# License:             BSD-3-Clause
# Author:              valery brinnel <firstname.lastname@gmail.com>
# Date:                07.05.2021
# Last Modified Date:  21.11.2021
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

from typing import Literal, Any
from collections.abc import Sequence
from ampel.types import Tag, ChannelId
from ampel.base.AmpelABC import AmpelABC
from ampel.base.decorator import abstractmethod
from ampel.base.AmpelBaseModel import AmpelBaseModel
from ampel.abstract.AbsDocIngester import AbsDocIngester
from ampel.content.MetaRecord import MetaRecord
from ampel.content.MetaActivity import MetaActivity
from ampel.enum.MetaActionCode import MetaActionCode


# Alias
ActivityRegister = dict[
    # activity stripped out of channel
	frozenset[tuple[str, Any]],
    # None: activity is not channel-bound, set[ChannelId]: will be added to MetaActivity during commit
	None | set[ChannelId]
]


class AbsCompiler(AmpelABC, AmpelBaseModel, abstract=True):

	origin: None | int = None
	tier: Literal[-1, 0, 1, 2, 3]
	run_id: int
	tag: None | Tag | Sequence[Tag]


	def __init__(self, **kwargs) -> None:

		super().__init__(**kwargs)
		self._tag = None
		self._ingest_tag_activity: None | MetaActivity = None

		if self.tag:
			self._ingest_tag_activity = {'action': MetaActionCode.ADD_INGEST_TAG}
			if isinstance(self.tag, (str, int)):
				self._tag = [self.tag]
				self._ingest_tag_activity['tag'] = self.tag
			else:
				self._tag = list(self.tag)
				self._ingest_tag_activity['tag'] = self._tag


	@abstractmethod(var_args=True)
	def add(self) -> None:
		...


	@abstractmethod
	def commit(self, ingester: AbsDocIngester, now: int | float, **kwargs) -> None:
		...


	def register_meta_info(self,
		ar: ActivityRegister,
		extra_register: dict[str, Any], # meta_extra
		channel: ChannelId,
		activity: None | MetaActivity | list[MetaActivity] = None,
		meta_extra: None | dict[str, Any] = None
	) -> None:
		"""
		Note: We could support the type list[tuple[str, any]] for the parameter activity
		as the dict form is actually superfluous (frozenset(dict.items()) is used in the end)
		but the performance gain is negligible (~80ns) and it complicates static typing
		"""

		add_chan_registered = False

		if activity:

			for el in [activity] if isinstance(activity, dict) else activity:

				# Channel-bound activity
				if 'channel' in el:

					if el['action'] & MetaActionCode.ADD_CHANNEL:
						add_chan_registered = True

					# Strip out 'channel' from activity
					# (to be able to merge similar activities accross channels)
					x = frozenset([y for y in el.items() if y[0] != 'channel'])

					# activity is already associated with another channel
					if x in ar:

						# Should not happen unless ingestion handler has an erroneous behavior
						if (g := ar[x]) is None:
							# Raising an error is probably a bad idea actually
							raise ValueError("Channel-less / channel-bound activity conflict [0]")

						else:
							# Add current channel to previously registered activity
							if isinstance(el['channel'], (int, str)):
								g.add(el['channel'])
							else:
								g.update(el['channel'])
					else:
						ar[x] = {el['channel']} if isinstance(el['channel'], (int, str)) else set(el['channel'])

				# Channel-less activity
				else:

					# Build register key
					x = frozenset(el.items())

					# activity was already registered
					if x in ar:

						# Previously registered activity is associated with a channel
						if ar[x] is not None:
							# Raising an error is probably a bad idea actually
							raise ValueError("Channel-less / channel-bound activity conflict [1]")

					# register new channel-less activity
					else:
						# (dict key contains activity content, value is None as in channel-less)
						ar[x] = None

		if not add_chan_registered:

			# "Add chan" activity 'key' (used in activity dict below)
			add_chan = frozenset(
				[('action', MetaActionCode.ADD_CHANNEL | MetaActionCode.BUMP_STOCK_UPD)]
			)

			if z := ar.get(add_chan):
				z.add(channel)
			else:
				ar[add_chan] = {channel}

		if meta_extra:
			# No collision detection implemented yet
			extra_register.update(meta_extra)


	@staticmethod
	def _metactivity_key(activity: MetaActivity, skip_keys: None | set[str] = None) -> frozenset[tuple[str, Any]]:
		return frozenset(
			(
				k,
				tuple(v) if isinstance(v, list) else v
			) for k, v in activity.items()
			if skip_keys is None or k not in skip_keys
		)


	def new_meta_info(self,
		channel: ChannelId,
		activity: None | MetaActivity | list[MetaActivity] = None,
		meta_extra: None | dict[str, Any] = None
	) -> tuple[ActivityRegister, dict[str, Any]]: # activity register, meta_extra

		ar: ActivityRegister = {}
		add_chan_registered = False

		if activity:
			for el in [activity] if isinstance(activity, dict) else activity:
				if 'tag' in el and isinstance(el['tag'], list):
					el['tag'] = frozenset(el['tag']) # type: ignore
				if 'channel' in el: # Channel-bound activity
					if el['action'] & MetaActionCode.ADD_CHANNEL:
						add_chan_registered = True
					ar[self._metactivity_key(el, {'channel'})] = (
						{el['channel']} if isinstance(el['channel'], (int, str))
						else set(el['channel'])
					)
				else: # Channel-less activity
					ar[self._metactivity_key(el)] = None

		if not add_chan_registered:
			ar[
				frozenset(
					[('action', MetaActionCode.ADD_CHANNEL | MetaActionCode.BUMP_STOCK_UPD)]
				)
			] = {channel}

		return ar, meta_extra.copy() if meta_extra else {}


	def build_meta(self,
		d: dict[
			frozenset[tuple[str, Any]], # key: traceid
			tuple[ActivityRegister, dict[str, Any]] # activity register, meta_extra
		],
		now: int | float
	) -> tuple[list[MetaRecord], list[Tag]]:

		recs: list[MetaRecord] = []
		tags = set(self._tag) if self._tag else set()

		# v[1]: dict[traceid, (activity register, meta_extra)]
		for tid, t in d.items():

			mrec: MetaRecord = {
				'run': self.run_id,
				'ts': now,
				'tier': self.tier
			}

			# add meta extra (for example: alert: 12345678)
			mrec.update(t[1]) # type: ignore[typeddict-item]
			al = []

			for activity, chan in t[0].items():

				ad = dict(
					sorted(
						(
							list(activity) +
							[("channel", next(iter(chan)) if len(chan) == 1 else list(chan))]
						)
						if chan else activity
					)
				)

				if 'tag' in ad:
					if isinstance(ad['tag'], frozenset):
						ad['tag'] = list(ad['tag'])
						tags.update(ad['tag'])
					else:
						tags.add(ad['tag'])

				al.append(ad)

			# report ingest tag activity (only to first metarecord)
			if len(recs) == 0 and self._tag:
				al.append(self._ingest_tag_activity) # type: ignore[arg-type]

			mrec['activity'] = al # type: ignore[typeddict-item]
			mrec['traceid'] = dict(sorted(tid))
			recs.append(mrec)

		return recs, list(tags)
