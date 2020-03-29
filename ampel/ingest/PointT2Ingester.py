#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/ingest/PointT2Ingester.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 23.03.2020
# Last Modified Date: 23.03.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from time import time
from pymongo import UpdateOne
from typing import Sequence, Dict, Optional, Union, Literal, Tuple

from ampel.types import StockId, ChannelId, DataPointId
from ampel.utils.collections import try_reduce
from ampel.content.DataPoint import DataPoint
from ampel.t2.T2RunState import T2RunState
from ampel.abstract.ingest.AbsPointT2Ingester import AbsPointT2Ingester
from ampel.abstract.ingest.AbsT2Compiler import AbsT2Compiler
from ampel.ingest.PointT2Compiler import PointT2Compiler


class PointT2Ingester(AbsPointT2Ingester):

	compiler: AbsT2Compiler[Sequence[DataPoint], DataPointId] = PointT2Compiler()
	default_options: Dict[
		Literal['eligible'],
		Optional[Union[Literal['first', 'last', Tuple[int, int, int]]]]
	] = {"eligible": None} # None means all eligible


	def ingest(self,
		stock_id: StockId,
		datapoints: Sequence[DataPoint],
		chan_selection: Dict[ChannelId, Optional[Union[bool, int]]]
	) -> None:

		optimized_t2s = self.compiler.compile(datapoints, chan_selection)
		now = int(time())

		# Loop over t2 units to be created
		for (t2_id, run_config, link_id), chan_names in optimized_t2s.items():

			# Set of channel names
			eff_chan_names = list(chan_names) # pymongo requires list

			# Append update operation to bulk list
			self.updates_buffer.add_t2_update(
				UpdateOne(
					# Matching search criteria
					{
						'stock': stock_id,
						'unit': t2_id,
						'config': run_config,
						'link': link_id,
						'col': 't0'
					},
					{
						# Attributes set if no previous doc exists
						'$setOnInsert': {
							'stock': stock_id,
							'tag': self.tags,
							'unit': t2_id,
							'link': link_id,
							'config': run_config,
							'status': T2RunState.TO_RUN.value,
							'col': 't0'
						},
						# Journal and channel update
						'$addToSet': {
							'channel': {'$each': eff_chan_names},
							'journal': {
								'tier': self.tier,
								'dt': now,
								'channel': try_reduce(eff_chan_names)
							}
						}
					},
					upsert=True
				)
			)
