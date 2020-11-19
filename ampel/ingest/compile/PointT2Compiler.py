#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/ingest/compile/PointT2Compiler.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 14.12.2017
# Last Modified Date: 01.05.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import List, Dict, Set, Optional, Union, Sequence, Any, Literal, Tuple
from ampel.content.DataPoint import DataPoint
from ampel.type import ChannelId, DataPointId
from ampel.model.ingest.T2IngestModel import T2IngestModel
from ampel.abstract.ingest.AbsPointT2Compiler import AbsPointT2Compiler


class PointT2Compiler(AbsPointT2Compiler):
	"""
	Helper class capabable of generating a nested dict that is used as basis to create T2 documents.
	The generated structure is optimized: multiple T2 documents associated with the same datapoint
	accross different channels are merged into one single T2 document
	that references all corrsponding channels.
	"""

	def __init__(self) -> None:
		super().__init__()
		self.slices: Dict[Tuple[ChannelId, str, Optional[int]], List[slice]] = {}


	def set_ingest_options(self,
		channel: ChannelId, model: T2IngestModel, options: Dict[str, Any]
	) -> None:

		s = self.get_slice(options.get('eligible'))
		k = (channel, model.unit_id, model.config)

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
		elif arg == "all":
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
		chan_selection: List[Tuple[ChannelId, Union[bool, int]]],
		datapoints: Sequence[DataPoint]
	) -> Dict[Tuple[str, Optional[int], DataPointId], Set[ChannelId]]:
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

		Dict key element 1: unit id
		Dict key element 2: hashed dict value of the T2's config dict (123)
		Dict key element 3: datapoint id
		Dict values: set of channel ids
		"""

		t2s_eff: Dict[Tuple[str, Optional[int], DataPointId], Set[ChannelId]] = {}
		datapoints = list(reversed(datapoints))

		for chan, ingest_model in self.get_ingest_models(chan_selection):

			t2_id = ingest_model.unit_id
			config = ingest_model.config

			for s in self.slices[(chan, t2_id, config)]:
				for dp in datapoints[s]:
					k = (t2_id, config, dp['_id'])
					if k not in t2s_eff:
						t2s_eff[k] = {chan}
					else:
						t2s_eff[k].add(chan)

		return t2s_eff
