#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/ingest/compile/CompilerBase.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 23.03.2020
# Last Modified Date: 05.06.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Dict, Any, Union, Set, Generator, Tuple, List
from ampel.type import ChannelId
from ampel.base import AmpelABC, defaultmethod
from ampel.model.ingest.T2IngestModel import T2IngestModel


class CompilerBase(AmpelABC):
	"""
	T2 compilers optimize the ingestion of T2 documents into the DB.
	In particular, T2 documents shared among different channels are merged with each other.
	They must be configured first using the method `add_ingest_config`.
	Then, the method `compile` can be used to optimize the creation of T2 documents.
	"""


	@defaultmethod(check_super_call=True)
	def __init__(self) -> None:
		self.t2_models: Dict[ChannelId, List[T2IngestModel]] = {}
		self.channels: Set[ChannelId] = set()


	def add_ingest_model(self, channel: ChannelId, model: T2IngestModel) -> None:

		if channel not in self.t2_models:
			self.channels.add(channel)
			self.t2_models[channel] = []

		self.t2_models[channel].append(model)


	def set_ingest_options(self,
		channel: ChannelId, model: T2IngestModel, options: Dict[str, Any]
	) -> None:
		pass


	def get_ingest_models(self,
		chan_selection: List[Tuple[ChannelId, Union[bool, int]]]
	) -> Generator[Tuple[ChannelId, T2IngestModel], None, None]:
		"""
		Get T2 configurations corresponding to the given T0 filter results
		"""

		# loop through all channels,
		for chan, res in chan_selection:

			# Skip channels unknown to/undefined by this particular ingester
			if chan not in self.channels:
				continue

			# loop through state t2 unit ids to be scheduled for this channel
			for ingest_model in self.t2_models[chan]:

				# Filters can return an int value to filter out t2s by "group id"
				if not (res is True or res in ingest_model.group): # type: ignore
					continue

				yield chan, ingest_model
