#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/mongo/view/AbsMongoFlatMultiView.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 26.03.2021
# Last Modified Date: 28.03.2021
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import List, Any, Dict, Sequence
from ampel.type import ChannelId
from ampel.base import abstractmethod
from ampel.mongo.view.AbsMongoView import AbsMongoView

class AbsMongoFlatMultiView(AbsMongoView, abstract=True):
	"""
	Handles flat AND or OR connected type of channel-based views (any number of)
	No support for nested logic schema (such as {any_of: [A, all_of: {[B, C]}]})
	hence the 'flat' in class name
	"""

	channel: Sequence[ChannelId]


	@abstractmethod
	def get_first_match(self) -> Dict[str, Any]:
		...


	def stock(self) -> List[Dict[str, Any]]:

		return [
			self.get_first_match(),
			{
				'$project': {
					'tag': 1,
					'name': 1,
					'body': 1,
					'channel': {'$setIntersection': [self.channel, '$channel']},
					'journal': self.morph_journal(),
					'created': {chan: f'$created.{chan}' for chan in self.channel},
					'modified': {chan: f'$modified.{chan}' for chan in self.channel}
				}
			}
		]


	def t1(self) -> List[Dict[str, Any]]:

		return [
			self.get_first_match(),
			{'$set': {'channel': self.get_channel_intersection()}}
		]


	def t2(self) -> List[Dict[str, Any]]:

		return [
			self.get_first_match(),
			{
				'$set': {
					'channel': self.get_channel_intersection(),
					'journal': self.morph_journal()
				}
			}
		]


	def t3(self) -> List[Dict[str, Any]]:

		return [
			self.get_first_match(),
			{'$set': {'channel': self.get_channel_intersection()}}
		]


	def morph_journal(self) -> Dict[str, Any]:
		"""
		If channel is an array, reduce its value to the intersection between
		the defined channels and the requested one (should work with AND or OR channel projections).
		"""

		or_list = [
			{'$eq': [chan, '$$jentry.channel']}
			for chan in self.channel
		]

		return {
			'$filter': {
				'input': {
					'$map': {
						'input': "$journal",
						'as': "jentry",
						'in': {
							'$cond': {
								'if': {'$isArray': '$$jentry.channel'},
								'then': {
									'$let': {
										'vars': {
											'intersect': {'$setIntersection': [self.channel, '$$jentry.channel']},
										},
										'in': {
											'$switch': {
												'branches': [
													{
														'case': {'$eq': [{'$size': '$$intersect'}, 0]},
														'then': '$$REMOVE'
													},
													{
														'case': {'$eq': [{'$size': '$$intersect'}, 1]},
														'then': {'$mergeObjects': ['$$jentry', {'channel': {"$arrayElemAt": ['$$intersect', 0]}}]}
													},
													{
														'case': {'$gt': [{'$size': '$$intersect'}, 1]},
														'then': {'$mergeObjects': ['$$jentry', {'channel': '$$intersect'}]}
													}
												]
											}
										}
									}
								},
								'else': {
									'$cond': {
										'if': {'$or': or_list},
										'then': '$$jentry',
										'else': '$$REMOVE'
									},
								},
							}
						}
					}
				},
				'as': 'jentry',
				'cond': {'$ne': ['$$jentry', None]}
			}
		}


	def get_channel_intersection(self) -> Any:

		return {
			'$let': {
				'vars': {
					'intersect': {'$setIntersection': [self.channel, '$channel']},
				},
				'in': {
					'$cond': {
						'if': {'$eq': [{'$size': '$$intersect'}, 1]},
						'then': {"$arrayElemAt": ['$$intersect', 0]},
						'else': '$$intersect'
					},
				}
			}
		}
