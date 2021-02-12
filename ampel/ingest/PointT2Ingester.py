#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/ingest/PointT2Ingester.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 23.03.2020
# Last Modified Date: 11.02.2021
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from time import time
from pymongo import UpdateOne
from typing import Sequence, Dict, Optional, Union, Literal, Tuple, List
from ampel.type import StockId, ChannelId
from ampel.enum.T2SysRunState import T2SysRunState
from ampel.content.DataPoint import DataPoint
from ampel.ingest.compile.PointT2Compiler import PointT2Compiler
from ampel.abstract.ingest.AbsT2Ingester import AbsT2Ingester
from ampel.abstract.ingest.AbsPointT2Ingester import AbsPointT2Ingester
from ampel.abstract.ingest.AbsPointT2Compiler import AbsPointT2Compiler


class PointT2Ingester(AbsPointT2Ingester):

	compiler: AbsPointT2Compiler = PointT2Compiler()
	default_ingest_config: Dict[
		Literal['eligible'],
		Optional[Union[Literal['first', 'last', 'all'], Tuple[int, int, int]]]
	] = {"eligible": None} # None means all eligible


	def ingest(self,
		stock_id: StockId,
		datapoints: Sequence[DataPoint],
		chan_selection: List[Tuple[ChannelId, Union[bool, int]]]
	) -> None:

		optimized_t2s = self.compiler.compile(chan_selection, datapoints)
		now = int(time())

		# Loop over t2 units to be created
		for (t2_id, run_config, link_id), chans in optimized_t2s.items():

			jchan, chan_add_to_set = AbsT2Ingester.build_query_parts(chans)

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
							'status': T2SysRunState.NEW.value,
							'col': 't0'
						},
						# Journal and channel update
						'$addToSet': {
							'channel': chan_add_to_set,
							'journal': {
								'tier': self.tier,
								'dt': now,
								'channel': jchan
							}
						}
					},
					upsert=True
				)
			)
