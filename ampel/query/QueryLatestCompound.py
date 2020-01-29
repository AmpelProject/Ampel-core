#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/query/QueryLatestCompound.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 13.01.2018
# Last Modified Date: 10.12.2019
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import collections
from bson.int64 import Int64
from typing import Union, Sequence, Dict, Optional, Any

from ampel.utils.AmpelUtils import AmpelUtils
from ampel.model.operator.AnyOf import AnyOf
from ampel.model.operator.AllOf import AllOf
from ampel.model.operator.OneOf import OneOf
from ampel.query.QueryMatchSchema import QueryMatchSchema

class QueryLatestCompound:
	"""
	Lastest by mean of compound data length.
	Validity requirement: compound size should always increase with time.
	Especially: if a compound member is discarded, it should not be deleted
	"""

	@staticmethod
	def fast_query(
		tran_ids: Union[int, Int64, str, Sequence[Union[int, Int64, str]]], 
		channels: Optional[Union[int, str, Dict, AllOf, AnyOf, OneOf]] = None
	) -> Dict:
		"""
		| **Must be used on transients whose compounds were solely created by T0** \
		(i.e with no T3 compounds)
		| Should perform faster than general_query.

		:param tran_ids: transient id(s), query can/should be performed on multiple ids at once.

		:param channels: a single channel or a dict schema \
		(see :obj:`QueryMatchSchema <ampel.query.QueryMatchSchema>` for details. \
		None (no criterium) means all channels are considered. 

		:returns: a dict instance to be used with the mongoDB **aggregation** framework.

		Example:\n
		.. sourcecode:: python\n
			In []: cursor = col.aggregate(
				QueryLatestCompound.fast_query(
					ZTFUtils.to_ampel_id(
						['ZTF18aaayyuq', 'ZTF17aaagvng', 'ZTF18aaabikt']
					)
				)
			)

			In []: list(cursor)
			Out[]: 
			[
				{'_id': b'T6TG\x96\x80\x1d\x86\x9f\x11\xf2G\xe7\xf4\xe0\xc3', 'stock': 'ZTF18aaayyuq'},
 				{'_id': b'\xaaL|\x94?\xa4\xa1D\xbe\x0c[D\x9b\xc6\xe6o', 'stock': 'ZTF18aaabikt'},
 				{'_id': b'\xaaL|\x14?\xb4\xc3D\xd2\x2a?L\x9a\xa6\xa1o', 'stock': 'ZTF17aaagvng'}
			]
		"""

		# Robustness
		if isinstance(tran_ids, Sequence):
			if not AmpelUtils.check_seq_inner_type(tran_ids, (int, Int64, str)):
				raise ValueError("Elements in tran_ids must be of type str or int or Int64 (bson)")
		else:
			if not isinstance(tran_ids, (int, Int64, str)):
				raise ValueError("tran_ids must be of type str or int or Int64 (or sequence of these types)")

		query = {
			'stock': tran_ids if isinstance(tran_ids, (int, Int64, str))
			else {
				'$in': tran_ids if isinstance(tran_ids, list)
					else list(tran_ids)
			}
		}

		if channels is not None:
			QueryMatchSchema.apply_schema(
				query, 'channels', channels
			)

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
					'data': {
						'$first': '$$ROOT'
					}
				}
			},
			{
				'$replaceRoot': {
					'newRoot': '$data'
				}
			},
			{ 
				'$project': { 
					'len': 0
				}
			}
		]


	@staticmethod
	def general_query(
		tran_id: Union[int, Int64, str], 
		project: Optional[Dict[str, Any]] = None, 
		channels: Optional[Union[int, str, Dict, AllOf, AnyOf, OneOf]] = None
	) -> Dict:
		"""
		| Should work with any ampel transients.
		| A detailed explanation of each step of the aggregation \
		is documented in the python notebook "T3 get_lastest_compound"

		:param tran_id: transient id (int, Int64 or string). \
		Query **CANNOT** be performed on multiple ids at once.

		:param dict project: optional projection stage at the end of the aggregation

		:param channels: single channel or a dict schema \
		(see :obj:`QueryMatchSchema <ampel.query.QueryMatchSchema>` for details. \
		None (no criterium) means all channels are considered. 

		:returns: A dict instance intended to be used with the mongoDB **aggregation** framework.

		IMPORTANT NOTE: the following two examples show the output of the aggregation framework \
		from mongodb (i.e the output after having performed a DB query **using the output \
		of this fuunction as parameter**), they do not show the output of this function.

		**MONGODB** Output example 1:\n
		.. sourcecode:: python\n
			In []: list(
				col.aggregate(
					QueryLatestCompound.general_query('ZTF18aaayyuq')
				)
			)
			Out[]: 
			[
			  {
				'_id': b'T6TG\x96\x80\x1d\x86\x9f\x11\xf2G\xe7\xf4\xe0\xc3',
				  'added': 1520796310.496276,
				  'alDocType': 2,
				  'channels': ['HU_SN1'],
				  'lastppdt': 2458158.7708565,
				  'len': 12,
				  'data': [{'pp': 375300016315010040},
				   {'pp': 375320176315010034},
				   {'pp': 375337116315010046},
				   {'pp': 375356366315010056},
				   {'pp': 377293446315010009},
				   {'pp': 377313156315010027},
				   {'pp': 377334096315010020},
				   {'pp': 377376126315010004},
				   {'pp': 377416496315010000},
				   {'pp': 378293006315010001},
				   {'pp': 378334946315010000},
				   {'pp': 404270856315015007}],
				  'tier': 0,
				  'stock': 'ZTF18aaayyuq'
				}
			]

		**MONGODB** Output example 2:\n
		.. sourcecode:: python\n
			In []: list(
				col.aggregate(
					QueryLatestCompound.general_query(
						'ZTF18aaayyuq', project={'$project': {'stock':1}}
					)
				)
			)
			Out[]: 
				[{'_id': '5de2480f28bfca0bd3baae890cb2d2ae', 'stock': 'ZTF18aaayyuq'}]
		"""

		# Robustness
		if isinstance(tran_id, collections.Sequence) or \
		not isinstance(tran_id, (int, Int64, str)):
			raise ValueError(
				"Type of tran_id must be a string or an int (multi tran_id queries not supported)"
			)

		query = {'stock': tran_id}

		if channels is not None:
			QueryMatchSchema.apply_schema(
				query, 'channels', channels
			)

		ret = [
			{
				'$match': query
			},
			{
				'$group': {
					'_id': '$tier', 
					'latestAdded': {
						'$max': '$added'
					}, 
					'comp': {
						'$push': '$$ROOT'
					}
				}
			},
			{
				'$sort': {
					'latestAdded': -1
				}
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
							'if': {
								'$eq': ['$comp.tier', 0]
							},
							'then': '$comp.len',
							'else': '$comp.added'
						}
					}
				}
			},
			{
				'$sort': {
					'sortValueUsed': -1
				}
			},
			{
				'$limit': 1
			},
			{
				'$replaceRoot': {
					'newRoot': '$comp' 
				}
			}
		]

		if project is not None:
			ret.append(project)

		return ret
