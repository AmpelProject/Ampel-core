#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/ingest/StockT2Ingester.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 23.03.2020
# Last Modified Date: 24.03.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from time import time
from pymongo import UpdateOne
from typing import Dict, Optional, Union
from ampel.types import StockId, ChannelId
from ampel.utils.collections import try_reduce
from ampel.t2.T2RunState import T2RunState
from ampel.abstract.ingest.AbsStockT2Ingester import AbsStockT2Ingester
from ampel.abstract.ingest.AbsT2Compiler import AbsT2Compiler
from ampel.ingest.StockT2Compiler import StockT2Compiler


class StockT2Ingester(AbsStockT2Ingester):

	compiler: AbsT2Compiler[StockId, StockId] = StockT2Compiler()

	def ingest(self,
		stock_id: StockId,
		chan_selection: Dict[ChannelId, Optional[Union[bool, int]]]
	) -> None:

		optimized_t2s = self.compiler.compile(stock_id, chan_selection)
		now = int(time())


		# Loop over t2 units to be created
		for t2_id in optimized_t2s:

			# Loop over run settings
			for run_config in optimized_t2s[t2_id]:

				# Loop over stock Ids
				for stock_id in optimized_t2s[t2_id][run_config]:

					# Set of channel names
					eff_chan_names = list( # pymongo requires list
						optimized_t2s[t2_id][run_config][stock_id]
					)

					# Append update operation to bulk list
					self.updates_buffer.add_t2_update(
						UpdateOne(
							# Matching search criteria
							{
								'stock': stock_id,
								'unit': t2_id,
								'config': run_config,
							},
							{
								# Attributes set if no previous doc exists
								'$setOnInsert': {
									'stock': stock_id,
									'tag': self.tags,
									'unit': t2_id,
									'config': run_config,
									'status': T2RunState.TO_RUN.value,
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
