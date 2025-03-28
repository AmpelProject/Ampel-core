#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/mongo/update/MongoT0Ingester.py
# License:             BSD-3-Clause
# Author:              valery brinnel <firstname.lastname@gmail.com>
# Date:                23.05.2021
# Last Modified Date:  14.12.2021
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

from collections import defaultdict
from datetime import datetime
from typing import Any, Literal

from pymongo import UpdateMany, UpdateOne

from ampel.abstract.AbsDocIngester import AbsDocIngester
from ampel.content.DataPoint import DataPoint
from ampel.mongo.update.HasUpdatesBuffer import HasUpdatesBuffer
from ampel.mongo.utils import maybe_use_each
from ampel.types import DataPointId


class MongoT0Ingester(AbsDocIngester[DataPoint], HasUpdatesBuffer):
	""" Inserts `DataPoint` into the t0 collection  """

	#: If 1 or 2: raise DuplicateKeyError on attempts to upsert the same id with different bodies
	#: 1: partial check is performed (compatible with potential muxer projection)
	#: 2: strict check is performed
	extended_match: Literal[0, 1, 2] = 0
	update_ttl: bool = False

	def ingest(self, doc: DataPoint) -> None:

		match: dict[str, Any] = {'id': doc['id']}
		upd: dict[str, Any] = {
			'$addToSet': {
				'channel': maybe_use_each(doc['channel'])
			},
			'$push': {
				'meta': maybe_use_each(doc['meta']) # meta must be set by compiler
			}
		}

		if 'body' in doc:
			upd['$setOnInsert'] = {'body': doc['body']}
			if self.extended_match:
				if self.extended_match == 2:
					match['body'] = doc['body']
				else:
					for k, v in doc['body'].items():
						match['body.' + k] = v

		if 'origin' in doc:
			match['origin'] = doc['origin']

		if 'tag' in doc:
			upd['$addToSet']['tag'] = maybe_use_each(doc['tag'])

		if 'stock' in doc:
			upd['$addToSet']['stock'] = doc['stock'] if isinstance(doc['stock'], int | bytes | str) \
				else maybe_use_each(doc['stock'])

		if 'expiry' in doc:
			upd['$max'] = {'expiry': doc['expiry']}

		self.updates_buffer.add_t0_update(
			UpdateOne(match, upd, upsert=True)
		)

	def update_expiry(self, dps: dict[DataPointId, datetime]) -> None:
		by_ttl: dict[datetime, list[DataPointId]] = defaultdict(list)
		for k, v in dps.items():
			by_ttl[v].append(k)
		for expires, ids in by_ttl.items():
			self.updates_buffer.add_t0_update(
				UpdateMany(
					{'id': {'$in': ids}},
					{'$max': {'expiry': expires}}
				)
			)