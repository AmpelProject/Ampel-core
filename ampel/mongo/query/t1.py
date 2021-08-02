#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/mongo/query/t1.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 13.01.2018
# Last Modified Date: 20.06.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import collections
from bson.int64 import Int64
from typing import Union, Sequence, Dict, Optional, Any, List
from ampel.types import StockId, ChannelId, StrictIterable
from ampel.util.collections import check_seq_inner_type
from ampel.model.operator.AnyOf import AnyOf
from ampel.model.operator.AllOf import AllOf
from ampel.model.operator.OneOf import OneOf
from ampel.mongo.schema import apply_schema
from ampel.mongo.query.general import type_stock_id

"""
Lastest by mean of compound 'body' length.
Validity requirement: compound size should always increase with time.
Especially: if a compound member is discarded, it should not be deleted
"""

def latest_fast_query(
	stock: Union[StockId, StrictIterable[StockId]],
	channel: Optional[Union[ChannelId, Dict, AllOf[ChannelId], AnyOf[ChannelId], OneOf[ChannelId]]] = None
) -> List[Dict]:
	"""
	| **Must be used on transients whose compounds were solely created by T0** (i.e with no T3 compounds)
	| Should perform faster than general_query.

	:param stock: transient id(s), query can/should be performed on multiple ids at once.

	:param channel: a single channel or a dict schema.
	None (no criterium) means all channel are considered.

	:returns: a dict instance to be used with the mongoDB **aggregation** framework.

	Example:\n
	.. sourcecode:: python\n
		In []: list(col.aggregate(fast_query(to_ampel_id(['ZTF18aaayyuq', 'ZTF17aaagvng', 'ZTF18aaabikt']))))
		Out[]: [
			{'_id': b'T6TG\x96\x80\x1d\x86\x9f\x11\xf2G\xe7\xf4\xe0\xc3', 'stock': 'ZTF18aaayyuq'},
			{'_id': b'\xaaL|\x94?\xa4\xa1D\xbe\x0c[D\x9b\xc6\xe6o', 'stock': 'ZTF18aaabikt'},
			{'_id': b'\xaaL|\x14?\xb4\xc3D\xd2\x2a?L\x9a\xa6\xa1o', 'stock': 'ZTF17aaagvng'}
		]
	"""

	# Robustness
	if isinstance(stock, Sequence):
		if not check_seq_inner_type(stock, type_stock_id):
			raise ValueError("Elements in stock must be of type str or int or Int64 (bson)")
	else:
		if not isinstance(stock, type_stock_id):
			raise ValueError("stock must be of type str or int or Int64 (or sequence of these types)")

	query = {
		'stock': stock if isinstance(stock, type_stock_id) \
			else {'$in': stock if isinstance(stock, list) else list(stock)}
	}

	if channel is not None:
		apply_schema(query, 'channel', channel)

	return [
		{
			'$match': query
		},
		{
			'$project': {
				'stock': 1,
				'len': 1
			}
		},
		{
			'$sort': {
				'stock': 1,
				'len': -1
			}
		},
		{
			'$group': {
				'_id': '$stock',
				'body': {
					'$first': '$$ROOT'
				}
			}
		},
		{
			'$replaceRoot': {
				'newRoot': '$body'
			}
		},
		{
			'$project': {
				'len': 0
			}
		}
	]


def latest_general_query(
	single_stock: StockId,
	project: Optional[Dict[str, Any]] = None,
	channel: Optional[Union[ChannelId, Dict, AllOf[ChannelId], AnyOf[ChannelId], OneOf[ChannelId]]] = None
) -> List[Dict[str, Any]]:
	"""
	| Should work with any ampel transients.
	| A detailed explanation of each step of the aggregation \
	is documented in the python notebook "T3 get_lastest_compound"

	:param single_stock: transient id (int, Int64 or string). \
	Query **CANNOT** be performed on multiple ids at once.

	:param dict project: optional projection stage at the end of the aggregation

	:param channel: single channel or a dict schema \
	(see :obj:`QueryMatchSchema <ampel.query.QueryMatchSchema>` for details. \
	None (no criterium) means all channel are considered.

	:returns: A dict instance intended to be used with the mongoDB **aggregation** framework.

	IMPORTANT NOTE: the following two examples show the output of the aggregation framework \
	from mongodb (i.e the output after having performed a DB query **using the output \
	of this fuunction as parameter**), they do not show the output of this function.

	**MONGODB** Output example 1:\n
	.. sourcecode:: python\n
		In []: list(col.aggregate(general_query('ZTF18aaayyuq')))
		Out[]: [
			{
				'_id': b'T6TG\x96\x80\x1d\x86\x9f\x11\xf2G\xe7\xf4\xe0\xc3',
				'added': 1520796310.496276,
				'channel': ['HU_SN1'],
				'lastppdt': 2458158.7708565,
				'len': 12,
				'body': [
					{'id': 375300016315010040},
					{'id': 375320176315010034},
					{'id': 375337116315010046},
					{'id': 375356366315010056},
					{'id': 377293446315010009},
					{'id': 377313156315010027},
					{'id': 377334096315010020},
					{'id': 377376126315010004},
					{'id': 377416496315010000},
					{'id': 378293006315010001},
					{'id': 378334946315010000},
					{'id': 404270856315015007}
				],
				'tier': 0,
				'stock': 'ZTF18aaayyuq'
			}
		]

	**MONGODB** Output example 2:\n
	.. sourcecode:: python\n
		In []: list(col.aggregate(general_query('ZTF18aaayyuq', project={'$project': {'stock':1}})))
		Out[]: [{'_id': '5de2480f28bfca0bd3baae890cb2d2ae', 'stock': 'ZTF18aaayyuq'}]
	"""

	# Robustness
	if isinstance(single_stock, collections.Sequence) or \
		not isinstance(single_stock, (int, Int64, str)):
		raise ValueError(
			"Type of single_stock must be a string or an int (multi single_stock queries not supported)"
		)

	query = {'stock': single_stock}

	if channel is not None:
		apply_schema(query, 'channel', channel)

	ret: List[Dict[str, Any]] = [
		{
			'$match': query
		},
		{
			'$group': {
				'_id': '$tier',
				'latestAdded': {'$max': '$added'},
				'comp': {'$push': '$$ROOT'}
			}
		},
		{
			'$sort': {'latestAdded': -1}
		},
		{
			'$limit': 1
		},
		{
			'$unwind': '$comp'
		},
		{
			'$project': {
				'_id': 0,
				'comp': 1,
				'sortValueUsed': {
					'$cond': {
						'if': {'$eq': ['$comp.tier', 0]},
						'then': '$comp.len',
						'else': '$comp.added'
					}
				}
			}
		},
		{
			'$sort': {'sortValueUsed': -1}
		},
		{
			'$limit': 1
		},
		{
			'$replaceRoot': {'newRoot': '$comp'}
		}
	]

	if project is not None:
		ret.append(project)

	return ret
