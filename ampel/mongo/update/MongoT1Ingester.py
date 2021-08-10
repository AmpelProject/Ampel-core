#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/mongo/update/MongoT1Ingester.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 24.04.2021
# Last Modified Date: 27.05.2021
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from pymongo import UpdateOne
from typing import Dict, Any, Union
from ampel.mongo.utils import maybe_use_each
from ampel.content.T1Document import T1Document
from ampel.abstract.AbsDocIngester import AbsDocIngester


class MongoT1Ingester(AbsDocIngester[T1Document]):
	""" This class inserts `T1Document` into the t1 collection.  """

	def ingest(self, doc: T1Document, now: Union[int, float]) -> None:

		# Note: $setOnInsert does not retain key order
		set_on_insert: Dict[str, Any] = {'dps': doc['dps']}
		match: Dict[str, Any] = {
			'stock': doc['stock'],
			'link': doc['link']
		}

		if 'unit' in doc:
			set_on_insert['unit'] = doc['unit']
			match['unit'] = doc['unit']

		if 'config' in doc:
			match['config'] = doc['config']

		if 'origin' in doc:
			match['origin'] = doc['origin']

		if 'tag' in doc:
			set_on_insert['tag'] = doc['tag']

		if 'body' in doc:
			set_on_insert['body'] = doc['body']

		self.updates_buffer.add_t1_update(
			UpdateOne(
				match,
				{
					'$setOnInsert': set_on_insert,
					'$addToSet': {'channel': maybe_use_each(doc['channel'])},
					 # meta must be set by compiler
					'$push': {'meta': maybe_use_each(doc['meta'])}
				},
				upsert=True
			)
		)
