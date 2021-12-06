#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/mongo/update/MongoT0Ingester.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 23.05.2021
# Last Modified Date: 09.10.2021
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from pymongo import UpdateOne
from typing import Dict, Any
from ampel.mongo.utils import maybe_use_each
from ampel.content.DataPoint import DataPoint
from ampel.abstract.AbsDocIngester import AbsDocIngester


class MongoT0Ingester(AbsDocIngester[DataPoint]):
	""" Inserts `DataPoint` into the t0 collection  """

	#: raise DuplicateKeyError on attempts to upsert the same id with different bodies
	check_id_collision: bool = True

	def ingest(self, doc: DataPoint) -> None:

		match: Dict[str, Any] = {'id': doc['id']}
		upd: Dict[str, Any] = {
			'$addToSet': {
				'channel': maybe_use_each(doc['channel'])
			},
			'$push': {
				'meta': maybe_use_each(doc['meta']) # meta must be set by compiler
			}
		}

		if 'body' in doc:
			upd['$setOnInsert'] = {'body': doc['body']}
			if self.check_id_collision:
				match['body'] = doc['body']

		if 'origin' in doc:
			match['origin'] = doc['origin']

		if 'tag' in doc:
			upd['$addToSet']['tag'] = maybe_use_each(doc['tag'])

		if 'stock' in doc:
			upd['$addToSet']['stock'] = doc['stock'] if isinstance(doc['stock'], (int, bytes, str)) \
				else maybe_use_each(doc['stock'])

		self.updates_buffer.add_t0_update(
			UpdateOne(match, upd, upsert=True)
		)
