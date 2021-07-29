#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/mongo/update/MongoT2Ingester.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 14.12.2017
# Last Modified Date: 23.05.2021
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from pymongo import UpdateOne
from typing import Dict, Any, Union
from ampel.enum.DocumentCode import DocumentCode
from ampel.content.T2Document import T2Document
from ampel.mongo.utils import maybe_use_each
from ampel.abstract.AbsDocIngester import AbsDocIngester


class MongoT2Ingester(AbsDocIngester[T2Document]):

	def ingest(self, doc: T2Document, now: Union[int, float]) -> None:

		# Note: mongodb $setOnInsert does not retain key order
		set_on_insert: Dict[str, Any] = {'code': DocumentCode.NEW.value}

		match = {
			'stock': doc['stock'],
			'unit': doc['unit'],
			'config': doc['config'],
			'link': doc['link']
		}

		if 'origin' in doc:
			match['origin'] = doc['origin']

		if 'tag' in doc:
			set_on_insert['tag'] = doc['tag']

		if 'col' in doc:
			set_on_insert['col'] = doc['col']

		# Append update operation to bulk list
		self.updates_buffer.add_t2_update(
			UpdateOne(
				match,
				{
					'$setOnInsert': set_on_insert,
					'$addToSet': {'channel': maybe_use_each(doc['channel'])},
					'$push': {'meta': maybe_use_each(doc['meta'])} # meta must be set by compiler
				},
				upsert=True
			)
		)
