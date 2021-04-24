#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/t1/T1SimpleCombiner.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 01.01.2018
# Last Modified Date: 24.04.2021
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Sequence, Dict, Union, Tuple, Set
from ampel.type import Tag
from ampel.content.DataPoint import DataPoint
from ampel.content.T1Record import T1Record
from ampel.type import ChannelId, DataPointId
from ampel.abstract.AbsT1Unit import AbsT1Unit


class T1SimpleCombiner(AbsT1Unit):

	debug: bool = False

	def combine(self,
		datapoints: Sequence[DataPoint],
		channels: Sequence[ChannelId]
	) -> Dict[ChannelId, Tuple[Set[Tag], Sequence[Union[DataPointId, T1Record]]]]:
		"""
		:param datapoints: sequence of dict instances representing datapoints
		Note that a datapoint defined with policy is equivalent a different datapoint (meaning a datapoint with a different ID).
		A datapoint might for example return a different magnitude when associated with a policy.
		Datapoint with policies will result in a different effective payload and thus a different effective id
		"""

		gen = self.gen_sub_entry
		return {
			chan: (
				tags := set(), # type: ignore # type hint and walrus do not like each other
				[gen(dp, chan, tags) for dp in datapoints]
			)
			for chan in channels
		}


	def gen_sub_entry(self,
		dp: DataPoint, channel_name: ChannelId, tags: Set[Tag]
	) -> Union[DataPointId, T1Record]:
		"""
		Method can be overriden by subclasses.
		Known overriding class: ZiCompoundBuilder (distrib ampel-ZTF)
		returns Union[DataPointId, T1Record] and potential tags
		"""

		# Channel specific exclusion. dp["excl"] could look like this: ["HU_SN", "HU_GRB"]
		if "excl" in dp and channel_name in dp['excl']: # type: ignore
			tags.add('EXCLUDED_DATAPOINT')
			if self.debug:
				self.logger.debug("Excluding datapoint", extra={'id': dp['_id']})

			return {'id': dp['_id'], 'excl': 'channel'}

		return dp['_id']
