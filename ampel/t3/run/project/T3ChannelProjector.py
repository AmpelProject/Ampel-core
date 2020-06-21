#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/t3/run/project/T3ChannelProjector.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 07.01.2020
# Last Modified Date: 19.06.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Sequence, Any, Dict, List, Union, Optional
from ampel.type import ChannelId
from ampel.log import VERBOSE
from ampel.t3.run.project.T3BaseProjector import T3BaseProjector
from ampel.aux.ComboDictModifier import ComboDictModifier


class T3ChannelProjector(T3BaseProjector):

	channel: ChannelId

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

		journal_modifier = ComboDictModifier(
			logger = self.logger,
			unalterable = self.unalterable,
			freeze = self.freeze,
			modifications = [
				# Modified ex: {"HU_RANDOM": 3213143434, "HU_RAPID": 43789574389}
				ComboDictModifier.KeepOnlyModel(op="keep_only", key="modified", keep=self.channel),
				# Created ex: {"HU_RANDOM": 3213143434, "HU_RAPID": 43789574389}
				ComboDictModifier.KeepOnlyModel(op="keep_only", key="created", keep=self.channel),
				# Added ex: {"msg": "test", "tier":0, "channel": ["HU_RANDOM", "HU_RAPID"]}
				ComboDictModifier.FuncModifyModel(op="modify", key="journal", func=self.channel_projection),
				ComboDictModifier.FuncModifyModel(op="modify", key="channel", func=self.overwrite_root_channel)
			]
		)

		self.add_func_projector("stock", journal_modifier.apply, first=True)

		for key in ("t1", "t2", "log"):
			self.add_func_projector(key, self.channel_projection, first=True) # type: ignore


	def overwrite_root_channel(self, v: Union[ChannelId, Sequence[ChannelId]]) -> Optional[ChannelId]:
		if v == self.channel or self.channel in v: # type: ignore[operator]
			return self.channel
		return None


	def channel_projection(self, dicts: Sequence[Dict[str, Any]]) -> Sequence[Dict[str, Any]]:
		"""
		Filters out dict entries not associated with configured channel
		Note: debug ouput handled by super class
		"""

		chan = self.channel
		setitem = dict.__setitem__
		ret: List[Dict] = []

		if not dicts:
			return []

		for el in dicts:
			if elchan := el.get('channel'):
				if isinstance(elchan, (str, int)):
					if chan == elchan:
						ret.append(el)
				else:
					if chan in elchan:
						if self.unalterable:
							ret.append({**el, 'channel': chan})
						else:
							setitem(el, 'channel', chan)
							ret.append(el)

		return tuple(ret)
