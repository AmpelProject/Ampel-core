#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File				: Ampel-core/ampel/t3/supply/select/T3FilteringStockSelector.py
# License			: BSD-3-Clause
# Author			: Jakob van Santen <jakob.van.santen@desy.de>
# Date				: 02.08.2020
# Last Modified Date: 02.08.2020
# Last Modified By	: Jakob van Santen <jakob.van.santen@desy.de>

from itertools import islice
from typing import Sequence, Dict, List, Any, Union, Optional, Generator

from ampel.types import StockId
from ampel.t3.supply.select.T3StockSelector import T3StockSelector
from ampel.model.operator.AllOf import AllOf
from ampel.model.operator.AnyOf import AnyOf
from ampel.model.t3.T2FilterModel import T2FilterModel
from ampel.mongo.query.general import build_general_query
from ampel.mongo.utils import maybe_match_array


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
	chunk_size: int = 200

	# Override/Implement
	def fetch(self) -> Generator[Dict[str,Any], None, None]:

		# Execute query on T0 collection to get target stocks
		if not (cursor := super().fetch()):
			return None

		# Execute aggregation on T2 collection to get matching subset of stocks
		# NB: aggregate in chunks to avoid the 100 MB aggregation memory limit
		input_count = 0
		output_count = 0
		while (stock_ids := [doc['stock'] for doc in islice(cursor, self.chunk_size)]):
			count = 0
			for count, doc in enumerate(
				self.context.db.get_collection('t2').aggregate(
					self._t2_filter_pipeline(stock_ids),
				),
				1
			):
				yield doc

			input_count += len(stock_ids)
			output_count += count

		self.logger.info(f"{output_count}/{input_count} stocks passed filter criteria")



	def _build_match(self, f: Union[T2FilterModel, AllOf[T2FilterModel], AnyOf[T2FilterModel]]) -> Dict[str, Any]:
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
			{'$replaceRoot': {'newRoot': {'stock': '$_id'}}}
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
				'code': 0,
				'body': [{
					...
					'data': {'thing1': 3}
				}]
			},
			{
				'_id': 2,
				...
				'stock': 42,
				'unit': 'Unit2',
				'code': 0,
				'body': [{
					...
					'data': {'thing2': 7}
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
			'code': 0,
			'unit': maybe_match_array(_all_units(self.t2_filter)),
			**build_general_query(stock_ids, self.channel, self.tag)
		}

		return [
			# select t2 docs for target stocks
			{
				'$match': match
			},
			# find "latest" result for each stock and unit, where "latest" is
			# the document with the largest _id (creation time)
			{
				'$sort': {
					'stock': 1,
					'unit': 1,
					'_id': 1
				}
			},
			{'$unwind': '$body'},
			{
				'$group': {
					'_id': {
						'stock': '$stock',
						'unit': '$unit'
					},
					'data': {'$last': '$body'}
				}
			},
			# nest data under key named for unit, e.g.
			# {'unit': 'T2Unit', 'data': {'foo': 1}} -> {'T2Unit': {'foo': 1}}
			{
				'$replaceRoot': {
					'newRoot': {
						'_id': '$_id',
						'data': {
							'$arrayToObject': [
								[{'k': '$_id.unit', 'v': '$data'}]
							]
						}
					}
				}
			},
			{
				'$group': {
					'_id': '$_id.stock',
					'data': {'$mergeObjects': '$data'}
				}
			},
			# flatten document
			{'$set': {'data._id': '$_id'}},
			{
				'$replaceRoot': {
					'newRoot': '$data'
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
