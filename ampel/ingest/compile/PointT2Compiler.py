#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/ingest/PointT2Compiler.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 14.12.2017
# Last Modified Date: 22.03.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import List, Dict, Set, Optional, Union, Sequence, Any, Literal, Tuple
from ampel.content.DataPoint import DataPoint
from ampel.types import ChannelId, DataPointId
from ampel.logging.AmpelLogger import AmpelLogger
from ampel.model.T2IngestModel import T2IngestModel
from ampel.abstract.ingest.AbsT2Compiler import AbsT2Compiler


class PointT2Compiler(AbsT2Compiler[Sequence[DataPoint], Tuple[str, int, DataPointId]]):
	"""
	Helper class capabable of generating a nested dict that is used as basis to create T2 documents.
	The generated structure is optimized: multiple T2 documents associated with the same datapoint
	accross different channels are merged into one single T2 document
	that references all corrsponding channels.
	"""

	def __init__(self) -> None:
		self.t2_models: Dict[ChannelId, List[T2IngestModel]] = {}
		self.channels: Set[ChannelId] = set()
		self.slices: Dict[Tuple[ChannelId, str, int], List[slice]] = {}


	def add_ingest_config(self,
		chan: ChannelId,
		model: T2IngestModel,
		options: Dict[str, Any],
		logger: AmpelLogger
	):

		if chan not in self.t2_models:
			self.channels.add(chan)
			self.t2_models[chan] = []

		self.t2_models[chan].append(model)

		s = self.get_slice(options.get('eligible'))
		k = (chan, model.unit, model.config)

		if k in self.slices:
			# Avoid duplicated slices from bad config
			for el in self.slices[k]:
				if s.__reduce__()[1] == el.__reduce__()[1]:
					return
			self.slices[k].append(s)
		else:
			self.slices[k] = [s]


	def get_slice(self,
		arg: Optional[Union[Literal['first', 'last'], Tuple[int, int, int]]]
	) -> slice:
		"""
		:raises: ValueError if parameter 'arg' is invalid
		"""
		if arg is None:
			return slice(None)
		elif arg == "first":
			return slice(1)
		elif arg == "last":
			return slice(-1, -2, -1)
		elif isinstance(arg, tuple) and len(arg) == 3:
			return slice(*arg)
		else:
			raise ValueError(
				f"Unsupported value provided as slice parameter : {arg}"
			)


	def compile(self,
		datapoints: Sequence[DataPoint],
		chan_selection: Dict[ChannelId, Optional[Union[bool, int]]]
	) -> Dict[Tuple[str, int, DataPointId], Set[ChannelId]]:
		"""
		TLDR: This function computes and returns a dict structure helpful for creating T2 docs.
		This computation is required since:
		* A given alert can be accepted by one filter and be rejected by the other
		* T0 filters can return a customized set of T2 units to be run (group id)
		----------------------------------------------------------------------------------

		:param chan_selection: example: {"CHAN_SN": True, "CHAN_GRB": 1, "CHAN_BH": None}
		CHANNEL_SN accepted the alert and requests all associated T2s to be created
		CHANNEL_GRB accepted the alert and requests only T2s with group ID 1 to be created
		CHANNEL_BH rejected the alert

		This method will create the following dict:
		{
			(SNCOSMO, 123, 456): {"CHANNEL_SN"},
			(PHOTO_Z, 487, 2336): {"CHANNEL_SN", "CHANNEL_GRB"}
		}

		Dict key element 1: unit id
		Dict key element 2: hashed dict value of the T2's config dict (123)
		Dict key element 3: datapoint id
		Dict values: set of channel ids
		"""

		t2s_eff: Dict[Tuple[str, int, DataPointId], Set[ChannelId]] = {}

		for chan in chan_selection:

			# Skip None/False (current selection rejected transient)
			# or channels is unknown/undefined by this particular ingester
			if not chan_selection[chan] or chan not in self.channels:
				continue

			# loop through ingest models defined for this channel
			for ingest_model in self.t2_models[chan]:

				# chan_selection can filter out t2s by "group id"
				# Note: bool is a subclass of int, so True is an instance of int
				if not isinstance(chan_selection[chan], bool):
					if chan_selection[chan] not in ingest_model.group: # type: ignore
						continue

				t2_id = ingest_model.unit
				config = ingest_model.config

				for s in self.slices[(chan, t2_id, config)]:
					for dp in datapoints[s]:
						k = (t2_id, config, dp['_id'])
						if k not in t2s_eff:
							t2s_eff[k] = {chan}
						else:
							t2s_eff[k].add(chan)

		return t2s_eff

		# Output example:
		# {
		#	('PHOTO_Z', 123, 34234): {CHANNEL_SN, CHANNEL_LEN, CHANNEL_5},
		# 	('PHOTO_Z', 213424, 111): {CHANNEL_GRB}
		# }
