#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/mongo/update/MongoStockIngester.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 14.12.2017
# Last Modified Date: 24.05.2021
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Any, Union
from pymongo import UpdateOne
from ampel.abstract.AbsDocIngester import AbsDocIngester
from ampel.content.StockDocument import StockDocument
from ampel.mongo.utils import maybe_use_each
from ampel.util.collections import try_reduce


class MongoStockIngester(AbsDocIngester[StockDocument]):

	def ingest(self, doc: StockDocument, now: Union[int, float]) -> None:

		set_on_insert: dict[str,Any] = {'ts.any.tied': now}
		add_to_set = {'channel': maybe_use_each(doc['channel'])}

		if 'name' in doc:
			set_on_insert['name'] = doc['name']
			
		if 'tag' in doc:
			add_to_set['tag'] = maybe_use_each(doc['tag'])

		for el in doc['journal']:
			el['channel'] = try_reduce(el['channel'])

		upd = {
			'$addToSet': add_to_set,
			'$min': {f'ts.{chan}.tied': now for chan in doc['channel']},
			'$max': {f'ts.{chan}.upd': now for chan in doc['channel']},
			'$push': {'journal': maybe_use_each(doc['journal'])}
		}

		if set_on_insert:
			upd['$setOnInsert'] = set_on_insert

		# Insert/Update stock document into stock collection
		self.updates_buffer.add_stock_update(
			UpdateOne(
				{'stock': doc['stock'], 'origin': doc['origin']}
				if 'origin' in doc else {'stock': doc['stock']},
				upd,
				upsert=True
			)
		)
