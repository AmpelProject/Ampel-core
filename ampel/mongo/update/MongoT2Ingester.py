#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/mongo/update/MongoT2Ingester.py
# License:             BSD-3-Clause
# Author:              valery brinnel <firstname.lastname@gmail.com>
# Date:                14.12.2017
# Last Modified Date:  08.10.2021
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

from typing import Any

from pymongo import UpdateOne

from ampel.abstract.AbsDocIngester import AbsDocIngester
from ampel.content.T2Document import T2Document
from ampel.enum.DocumentCode import DocumentCode
from ampel.mongo.update.HasUpdatesBuffer import HasUpdatesBuffer
from ampel.mongo.utils import maybe_use_each


class MongoT2Ingester(AbsDocIngester[T2Document], HasUpdatesBuffer):

	def ingest(self, doc: T2Document) -> None:
		new = doc.get('code', DocumentCode.NEW) == DocumentCode.NEW

		# Note: mongodb $setOnInsert does not retain key order
		set_on_insert: dict[str, Any] = {'code': DocumentCode.NEW} if new else {}
		add_to_set: dict[str, Any] = {'channel': maybe_use_each(doc['channel'])}

		match = {
			'stock': doc['stock'],
			'unit': doc['unit'],
			'config': doc['config'],
			'link': doc['link']
		}

		if 'origin' in doc:
			match['origin'] = doc['origin']

		if 'col' in doc:
			set_on_insert['col'] = doc['col']

		if 'tag' in doc:
			add_to_set['tag'] = maybe_use_each(doc['tag'])

		# Append update operation to bulk list
		self.updates_buffer.add_t2_update(
			UpdateOne(
				match,
				{
					'$setOnInsert': set_on_insert,
					'$set': {} if new else {'code': doc['code']},
					'$addToSet': add_to_set,
					'$max': {'expiry': doc['expiry']} if 'expiry' in doc else {},
					'$push': {
						'meta': maybe_use_each(doc['meta']), # meta must be set by compiler
						'body': maybe_use_each(doc['body']),
					} 
				},
				upsert=True
			)
		)
