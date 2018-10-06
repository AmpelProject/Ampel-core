#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/db/query/QueryLatestCompound.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 13.01.2018
# Last Modified Date: 04.07.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import collections, bson
from ampel.core.flags.AlDocType import AlDocType
from ampel.core.flags.FlagUtils import FlagUtils
from ampel.pipeline.common.AmpelUtils import AmpelUtils
from ampel.pipeline.db.query.QueryMatchFlags import QueryMatchFlags
from ampel.pipeline.db.query.QueryMatchCriteria import QueryMatchCriteria

class QueryLatestCompound:
	"""
	"""

	@staticmethod
	def fast_query(tran_ids, channels=None):
		"""
		Note:
		-----
		! Must be used on transients whose compounds were solely created by T0 !
		(i.e with no T3 compounds)

		Returns:
		--------
		A dict instance to be used with the mongoDB *aggregation* framework.

		Example:
		--------
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
			{'_id': Binary(b'T6TG\x96\x80\x1d\x86\x9f\x11\xf2G\xe7\xf4\xe0\xc3', 5), 'tranId': 'ZTF18aaayyuq'},
 			{'_id': Binary(b'\xaaL|\x94?\xa4\xa1D\xbe\x0c[D\x9b\xc6\xe6o', 5), 'tranId': 'ZTF18aaabikt'},
 			{'_id': Binary(b'\xaaL|\x14?\xb4\xc3D\xd2\x2a?L\x9a\xa6\xa1o', 5), 'tranId': 'ZTF17aaagvng'}
		]

		Parameters	
		----------
		tran_id: transient id(s) (string, list of string, set of strings). 
		Query can be performed on multiple ids at once.

		channels: 
			-> a list of strings (whereby string = channel id)
			-> a 2d list of strings 
				* outer list: flag ids connected by OR
				* innerlist: flag ids connected by AND

		Should perform faster than general_query.
		"""

		# Robustness
		type_tran_ids = type(tran_ids)
		if type_tran_ids in (list, tuple, set):
			if not AmpelUtils.check_seq_inner_type(tran_ids, (int, str, bson.int64.Int64)):
				raise ValueError("Elements of tran_ids sequence must be have type str or int")
		elif not type_tran_ids in (int, str, bson.int64.Int64):
			raise ValueError("tran_ids must have type str or int (or sequence of these types)")

		match_dict = {
			'alDocType': AlDocType.COMPOUND
		}

		match_dict['tranId'] = ( 
			tran_ids if type(tran_ids) in (str, int)
			else {'$in': tran_ids if type(tran_ids) is list else list(tran_ids)}
		)

		if channels is not None:
			QueryMatchCriteria.add_from_list(
				match_dict, 'channels',
				(channels if not FlagUtils.contains_enum_flag(channels) 
				else FlagUtils.enum_flags_to_lists(channels)), 
			)

		return [
			{
				'$match': match_dict
			},
			{
				'$project': {
					'tranId': 1,
					'len': 1
				}
			},
			{
				'$sort': {
					'tranId': 1, 
					'len': -1
				} 
			},
			{
				'$group': {
					'_id': '$tranId',
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
	def general_query(tran_id, project=None, channels=None):
		"""
		Can be used on any ampel transients.
		There is a very detailed explanation of each step of the aggragetion 
		documented in the python notebook "T3 get_lastest_compound"

		Returns:
		--------
		A dict instance to be used with the mongoDB *aggregation* framework.

		Parameters	
		----------
		tran_id: transient id (string). Query can *not* be done on multiple ids at once.
		project: optional projection stage at the end of the aggregation (dict instance)
		channels: see 'fast_query' docstring

		Examples:
		---------
		IMPORTANT NOTE: the following two examples show the output of the aggregation framework 
		from mongodb (i.e the output after having performed a DB query *using the output 
		of this fuunction as parameter*), they do not show the output of this function.

		*MONGODB* Output example 1:
		---------------------------
		In []: list(
			col.aggregate(
				QueryLatestCompound.general_query('ZTF18aaayyuq')
			)
		)
		Out[]: 
		[
		  {
			'_id': Binary(b'T6TG\x96\x80\x1d\x86\x9f\x11\xf2G\xe7\xf4\xe0\xc3', 5),
			  'added': 1520796310.496276,
			  'alDocType': 2,
			  'channels': ['HU_SN1'],
			  'lastppdt': 2458158.7708565,
			  'len': 12,
			  'pps': [{'pp': 375300016315010040},
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
			  'tranId': 'ZTF18aaayyuq'
			}
		]

		*MONGODB* Output example 2:
		---------------------------
		In []: list(
			col.aggregate(
				QueryLatestCompound.general_query(
					'ZTF18aaayyuq', project={'$project': {'tranId':1}}
				)
			)
		)
		Out[]: 
			[{'_id': '5de2480f28bfca0bd3baae890cb2d2ae', 'tranId': 'ZTF18aaayyuq'}]
		"""

		# Robustness
		if isinstance(tran_id, collections.Sequence) or not type(tran_id) in (str, int):
			raise ValueError("Type of tran_id must be a string or an int (multi tran_id queries not supported)")

		match_dict = {
			'tranId': tran_id, 
			'alDocType': AlDocType.COMPOUND
		}

		if channels is not None:
			QueryMatchCriteria.add_from_list(
				match_dict, 'channels',
				(channels if not FlagUtils.contains_enum_flag(channels) 
				else FlagUtils.enum_flags_to_lists(channels)), 
			)

		ret = [
			{
				'$match': match_dict
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
