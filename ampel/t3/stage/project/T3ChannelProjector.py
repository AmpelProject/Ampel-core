#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/t3/stage/project/T3ChannelProjector.py
# License:             BSD-3-Clause
# Author:              valery brinnel <firstname.lastname@gmail.com>
# Date:                07.01.2020
# Last Modified Date:  22.11.2020
# Last Modified By:    Jakob van Santen <jakob.van.santen@desy.de>

from typing import Any
from collections.abc import Sequence
from ampel.types import ChannelId
from ampel.log import VERBOSE
from ampel.util.logicschema import reduce_to_set
from ampel.model.operator.AllOf import AllOf
from ampel.model.operator.AnyOf import AnyOf
from ampel.model.operator.OneOf import OneOf
from ampel.aux.ComboDictModifier import ComboDictModifier
from ampel.t3.stage.project.T3BaseProjector import T3BaseProjector


class T3ChannelProjector(T3BaseProjector):

	channel: ChannelId | AllOf[ChannelId] | AnyOf[ChannelId] | OneOf[ChannelId]

	# Whether to cast structures into immutables objects after modification
	freeze: bool = True

	# Whether fields from input ampel_buffer can be directly altered/modified
	# This is the case if the parent T3UnitRunner contains only one run block
	# If this is not the case, new dict instances must be created to modify existing dicts
	unalterable: bool = True


	def __init__(self, **kwargs) -> None:
		"""
		:param channel: the channel id of the channel for which projections should be performed
		:param unalterable: see ComboDictModifier docstring
		:param freeze: see ComboDictModifier docstring
		"""

		super().__init__(**kwargs)

		self.verbose = self.logger.verbose
		if self.verbose:
			self.logger.log(VERBOSE, f"Setting up channel project for '{self.channel}'")
		self._channel_set: set[ChannelId] = reduce_to_set(self.channel)

		journal_modifier = ComboDictModifier(
			logger = self.logger,
			unalterable = self.unalterable,
			freeze = self.freeze,
			modifications = [
				# Modified ex: {"HU_RANDOM": 3213143434, "HU_RAPID": 43789574389}
				ComboDictModifier.KeepOnlyModel(op="keep_only", key="ts", keep=list(self._channel_set)),
				# Added ex: {"msg": "test", "tier":0, "channel": ["HU_RANDOM", "HU_RAPID"]}
				ComboDictModifier.FuncModifyModel(op="modify", key="journal", func=self.channel_projection),
				ComboDictModifier.FuncModifyModel(op="modify", key="channel", func=self.overwrite_root_channel)
			]
		)

		self.add_func_projector("stock", journal_modifier.apply, first=True)

		for key in ("t1", "t2"):
			self.add_func_projector(key, self.channel_projection, first=True) # type: ignore


	def overwrite_root_channel(self, v: Sequence[ChannelId]) -> None | Sequence[ChannelId]:
		if subset := list(self._channel_set.intersection(v)):
			return subset
		else:
			return None


	def channel_projection(self, dicts: Sequence[dict[str, Any]]) -> Sequence[dict[str, Any]]:
		"""
		Filters out dict entries not associated with configured channel
		Note: debug ouput handled by super class
		"""

		channel_set = self._channel_set
		setitem = dict.__setitem__
		ret: list[dict] = []

		if not dicts:
			return []

		for el in dicts:
			if elchan := el.get('channel'):
				if isinstance(elchan, (str, int)):
					if elchan in channel_set:
						ret.append(el)
				else:
					if subset := list(channel_set.intersection(elchan)):
						channels = (
							list(subset)
							if len(subset) > 1 else list(subset)[0]
						)
						if self.unalterable:
							ret.append({**el, 'channel': channels})
						else:
							setitem(el, 'channel', channels)
							ret.append(el)

		return tuple(ret)
