#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/dbquery/LatestCompoundQuery.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 13.01.2018
# Last Modified Date: 11.03.2018

from ampel.flags.AlDocTypes import AlDocTypes
from ampel.flags.FlagUtils import FlagUtils

from ampel.pipeline.db.query.MatchFlagsQuery import MatchFlagsQuery
from ampel.pipeline.db.query.MatchCriteriaQuery import MatchCriteriaQuery

class LatestCompoundQuery:
	"""
	"""

	@staticmethod
	def fast_query(tran_ids, channels=None):
		"""
		channels: can be a flag, a list of flags, a string, or a list of strings:

			flags:
			Please see the doctring of FlagUtils.enum_flags_to_dbquery for more info.
			-> either an instance of ampel.flags.ChannelFlags 
			  (whereby the flags contained in on instance are 'AND' connected)
			-> or list of instances of ampel.flags.ChannelFlags whereby the instances 
			   are 'OR' connected between each other

			strings:
			-> a list of strings (whereby string = channel id)
			-> a 2d list of strings (TODO: explain)

		Should perform faster than latest_compound_general_query.
		Must be used on transients with compounds solely created by T0 
		(i.e no T3 compounds)
		"""

		if not type(tran_ids) is list:
			if not type(tran_ids) is str:
				raise ValueError("Type of tran_ids must be either a string or a list of strings")

		match_dict = {
			'alDocType': AlDocTypes.COMPOUND
		}

		match_dict['tranId'] = ( 
			tran_ids if type(tran_ids) is str or len(tran_ids) == 1
			else {'$in': tran_ids}
		)

		if channels is not None:
			MatchCriteriaQuery.add_match_from_list(
				(channels if not FlagUtils.contains_enum_flag(channels) 
				else FlagUtils.enum_flags_to_lists(channels)), 
				match_dict, 'channels'
			)

		return [
			{
				'$match': match_dict
			},
			{
				'$project': {
					'tranId': 1,
					'len': 1,
					'compoundId': 1
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
	def general_query(tran_id, channels=None):

		if type(tran_id) is list:
			raise ValueError("Type of tran_ids must be string (multi tranId queries not supported)")

		match_dict = {
			'tranId': tran_id, 
			'alDocType': AlDocTypes.COMPOUND
		}

		if channels is not None:
			MatchCriteriaQuery.add_match_from_list(
				(channels if not FlagUtils.contains_enum_flag(channels) 
				else FlagUtils.enum_flags_to_lists(channels)), 
				match_dict, 'channels'
			)

		print(match_dict)

		return [
			{
				'$match': match_dict
			},
			{
				'$group': {
					'_id': '$tier', 
					'latestAdded': {
						'$max': '$added'
					}, 
					'compound': {
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
				'$unwind': '$compound'
			},
			{
				'$project': {
					'_id': 0,
					'compound': 1, 
					'sortValueUsed': {
						'$cond': {
							'if': {
								'$eq': ['$compound.tier', 0]
							},
							'then': '$compound.len',
							'else': '$compound.added'
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
					'newRoot': '$compound' 
				}
			}
		]
