#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/ingest/StockIngester.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 14.12.2017
# Last Modified Date: 09.06.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from time import time
from pymongo import UpdateOne
from typing import Tuple, Dict, List, Any, Union, Literal, Optional
from ampel.type import StockId, ChannelId
from ampel.abstract.ingest.AbsStockIngester import AbsStockIngester
from ampel.util.mappings import flatten_dict


class StockIngester(AbsStockIngester):

	# setOnInsert tag
	tag: Optional[List[Union[int, str]]]
	tier: Literal[0, 1, 3] = 0


	def __init__(self, **kwargs):
		super().__init__(**kwargs)
		self.set_on_insert = {'tag': self.tag}


	def ingest(self,
		stock_id: StockId,
		chan_selection: List[Tuple[ChannelId, Union[bool, int]]],
		jextra: Dict[str, Any]
	) -> None:
		"""
		Update transient document
		:param jextra: includes provided parameter into the journal extra's field
		"""

		now = int(time())
		created = {'any': now}
		modified = {'any': now}
		jchan: List[ChannelId] = []

		# loop through all channels,
		for chan, res in chan_selection:
			# json requires dict keys to be str
			if isinstance(chan, int):
				c = str(chan)
				created[c] = now
				modified[c] = now
			else:
				created[chan] = now
				modified[chan] = now
			jchan.append(chan)

		if len(jchan) == 1:
			jchan = jchan[0] # type: ignore[assignment]
			chan_add_to_set = jchan
		else:
			chan_add_to_set = {'$each': jchan} # type: ignore[assignment]

		# Insert/Update transient document into stock collection
		self.updates_buffer.add_stock_update(
			UpdateOne(
				{'_id': stock_id},
				{
					'$addToSet': {'channel': chan_add_to_set},
					'$setOnInsert': self.get_setOnInsert(stock_id),
					'$min': flatten_dict({'created': created}),
					'$set': flatten_dict({'modified': modified}),
					'$push': {
						'journal': {
							'tier': self.tier,
							'ts': now,
							'channel': jchan,
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
