#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File				: Ampel-core/ampel/t3/select/T3FilteringStockSelector.py
# License			: BSD-3-Clause
# Author			: Jakob van Santen <jakob.van.santen@desy.de>
# Date				: 02.08.2020
# Last Modified Date: 02.08.2020
# Last Modified By	: Jakob van Santen <jakob.van.santen@desy.de>

from pymongo.cursor import Cursor
from typing import Sequence, Dict, List, Any, Union, Optional

from ampel.type import StockId
from ampel.t3.select.T3StockSelector import T3StockSelector
from ampel.model.operator.AllOf import AllOf
from ampel.model.operator.AnyOf import AnyOf
from ampel.model.operator.OneOf import OneOf
from ampel.model.t3.T2FilterModel import T2FilterModel
from ampel.db.query.general import build_general_query
from ampel.db.query.utils import match_array


class T3FilteringStockSelector(T3StockSelector):
	"""
	Selector subclass that filters stocks based on their latest T2 results.
	
	Example configuration::
		
		{
			...
			"t2_filter": [
				{
					"unit": "T2SNCosmo",
					"match": {
						"fit_acceptable": True,
						"fit_results.c": {"$gt": 1},
					},
				}
			]
		}
	"""

	t2_filter: Union[T2FilterModel, AllOf[T2FilterModel], AnyOf[T2FilterModel]]

	# Override/Implement
	def fetch(self) -> Optional[Cursor]:

		# Execute query on T0 collection to get target stocks
		if cursor := super().fetch():
			stock_ids = [doc['_id'] for doc in cursor]
		else:
			return None

		# Execute aggregation on T2 collection to get matching subset of stocks
		cursor = self.context.db.get_collection('t2').aggregate(
			self._t2_filter_pipeline(stock_ids)
		)

		return cursor


	def _build_match(self, f: Union[T2FilterModel, AllOf[T2FilterModel], AnyOf[T2FilterModel]]) -> Dict[str,Any]:
		if isinstance(f, T2FilterModel):
			return {f"{f.unit}.{k}": v for k, v in f.match.items()}
		elif isinstance(f, AllOf):
			return {'$and': [self._build_match(el) for el in f.all_of]}
		elif isinstance(f, AnyOf):
			return {'$or': [self._build_match(el) for el in f.any_of]}
		else:
			raise TypeError()


	def _t2_filter_pipeline(self, stock_ids: List[StockId]) -> List[Dict]:
		return self._t2_merge_pipeline(stock_ids) + [
			{'$match': self._build_match(self.t2_filter)},
			{'$project': {'_id': 1}}
		]


	def _t2_merge_pipeline(self, stock_ids: List[StockId]) -> List[Dict[str, Any]]:
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
		
		the pipeline yields a single doc of the form::
			
			{
				'_id': 42,
				'Unit1': {'thing1': 3},
				'Unit2': {'thing2': 7}
			}
		"""
		# NB: we reuse the general query here to ensure that we only process
		# T2s associated with the requested channels
		match = {
			'status': 0,
			'unit': match_array(_all_units(self.t2_filter)),
			**build_general_query(stock_ids, self.channel, self.tag)
		}

		return [
			# select t2 docs for target stocks
			{
				'$match': match
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

def _all_units(filters: Union[T2FilterModel, AllOf[T2FilterModel], AnyOf[T2FilterModel]]) -> List[str]:
	"""
	Get the set of all units involved in the selection
	"""
	if isinstance(filters, T2FilterModel):
		return [filters.unit]
	elif isinstance(filters, AllOf):
		return list(set(f.unit for f in filters.all_of))
	elif isinstance(filters, AnyOf):
		# NB: AnyOf may contain AllOf
		return list(set(sum((_all_units(f) for f in filters.any_of), [])))
	else:
		raise TypeError()
