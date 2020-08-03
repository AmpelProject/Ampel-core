#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File				: Ampel-core/ampel/t3/select/T3FilteringStockSelector.py
# License			: BSD-3-Clause
# Author			: Jakob van Santen <jakob.van.santen@desy.de>
# Date				: 02.08.2020
# Last Modified Date: 02.08.2020
# Last Modified By	: Jakob van Santen <jakob.van.santen@desy.de>

from pymongo.cursor import Cursor
from typing import Sequence, Dict, List, Any, Union
from pydantic import validator

from ampel.t3.select.T3StockSelector import T3StockSelector
from ampel.model.t3.T2FilterModel import T2FilterModel

class T3FilteringStockSelector(T3StockSelector):
	"""
	Selector subclass that filters stocks based on their latest T2 results.
	Example:
	.. sourcecode:: python\n
	{
		...
		"t2_filter": [
			{
				"unit": "T2SNCosmo",
				"match": {
					"fit_acceptable": True,
					"fit_results.c": {"$gt": 1},
				},
			},
			
		]
	}
	"""

	t2_filter: Sequence[T2FilterModel]

	# Override/Implement
	def fetch(self) -> Cursor:
		""" The returned Iterator is a pymongo Cursor """

		# Execute query on T0 collection to get target stocks
		stock_ids = [doc['_id'] for doc in super().fetch()]

		# Extract parameters for T2 query
		target_t2s = list(set(f.unit for f in self.t2_filter))
		match_doc = {f"{f.unit}.{k}": v for f in self.t2_filter for k,v in f.match.items()}

		# Execute aggregation on T2 collection to get matching subset of stocks
		pipeline = self._t2_filter_pipeline(stock_ids, target_t2s, match_doc)
		cursor = self.context.db.get_collection('t2').aggregate(
			pipeline
		)

		return cursor

	def _t2_filter_pipeline(self, stock_ids: Sequence, target_t2s: Sequence[str], match_doc: Dict) -> List[Dict]:
		merge = self._t2_merge_pipeline(stock_ids, target_t2s)
		return merge + [
			{'$match': match_doc},
			{'$project': {'_id': 1}}
		]

	def _t2_merge_pipeline(self, stock_ids: Sequence, target_t2s: Sequence[str]) -> List[Dict[str, Any]]:
		"""
		Create a pipeline for the T2 collection that yields docs whose _id is
		the stock id and whose remaining fields are the latest result for each
		T2. For example, given the following T2 docs::
			{
				'_id': 1,
				...
				'stock': 42,
				'unit': 'Unit1',
				'status': 0,
				'body': [{
					...
					'results': {'thing1': 3}
				}]
			},
			{
				'_id': 2,
				...
				'stock': 42,
				'unit': 'Unit2',
				'status': 0,
				'body': [{
					...
					'results': {'thing2': 7}
				}]
			}
		the pipeline yields a single doc of the form:
			{
				'_id': 42,
				'Unit1': {'thing1': 3},
				'Unit2': {'thing2': 7}
			}
		"""
		return [
			# select t2 docs for target stocks
			{
				'$match': {
					'stock': {
						'$in': stock_ids
					},
					'status': 0,
					'unit': {
						'$in': target_t2s
					}
				}
			},
			# find latest result for each stock and unit
			{
				'$sort': {
					'stock': 1,
					'unit': 1,
					'link': 1
				}
			},
			{'$unwind': '$body'},
			{
				'$group': {
					'_id': {
						'stock': '$stock',
						'unit': '$unit'
					},
					'result': {'$last': '$body.result'}
				}
			},
			# nest result under key named for unit, e.g.
			# {'unit': 'T2Unit', 'result': {'foo': 1}} -> {'T2Unit': {'foo': 1}}
			{
				'$replaceRoot': {
					'newRoot': {
						'_id': '$_id',
						'result': {
							'$arrayToObject': [
								[{'k': '$_id.unit', 'v': '$result'}]
							]
						}
					}
				}
			},
			{
				'$group': {
					'_id': '$_id.stock',
					'result': {'$mergeObjects': '$result'}
				}
			},
			# flatten document
			{'$set': {'result._id': '$_id'}},
			{
				'$replaceRoot': {
					'newRoot': '$result'
				}
			}
		]
