#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/ingest/StockDefaultIngester.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 14.12.2017
# Last Modified Date: 18.03.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from time import time
from pymongo import UpdateOne
from typing import Optional, Dict, List, Any, Union, Iterable, Literal
from ampel.types import StockId, ChannelId
from ampel.utils.collections import try_reduce
from ampel.abstract.ingest.AbsStockIngester import AbsStockIngester

class StockDefaultIngester(AbsStockIngester):

	# setOnInsert tag
	tag: Optional[List[Union[int, str]]]
	tier: Literal[0, 1, 3] = 0


	def __init__(self, **kwargs):
		super().__init__(**kwargs)
		self.set_on_insert = {'tag': self.tag}


	def ingest(self,
		stock_id: StockId,
		chan_names: Iterable[ChannelId],
		jextra: Dict[str, Any]
	) -> None:
		"""
		Update transient document
		:param jextra: includes provided parameter into the journal extra's field
		"""

		now = int(time())

		# Insert/Update transient document into 'transients' collection
		self.updates_buffer.add_stock_update(
			UpdateOne(
				{'_id': stock_id},
				{
					'$addToSet': {
						'channel': next(iter(chan_names)) if len(chan_names) == 1 \
							else {'$each': chan_names}
					},
					'$setOnInsert': self.get_setOnInsert(stock_id),
					'$min': {
						f'created.{chan_name}': now
						for chan_name in [*chan_names, 'Any']
					},
					'$set': {
						f'modified.{chan_name}': now
						for chan_name in [*chan_names, 'Any']
					},
					'$push': {
						'journal': {
							'tier': self.tier,
							'dt': now,
							'channel': try_reduce(chan_names),
							'run': self.run_id,
							'extra': jextra
						}
					}
				},
				upsert=True
			)
		)


	def get_setOnInsert(self, stock_id: StockId) -> Dict[str, Any]:
		""" To be overriden by sub-classes """
		return self.set_on_insert
