#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/mongo/view/AbsMongoFlatMultiView.py
# License:             BSD-3-Clause
# Author:              valery brinnel <firstname.lastname@gmail.com>
# Date:                26.03.2021
# Last Modified Date:  06.10.2021
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

from typing import Any
from collections.abc import Sequence
from ampel.types import ChannelId
from ampel.base.decorator import abstractmethod
from ampel.mongo.view.AbsMongoView import AbsMongoView


class AbsMongoFlatMultiView(AbsMongoView, abstract=True):
	"""
	Handles flat AND or OR connected type of channel-based views (any number of)
	No support for nested logic schema (such as {any_of: [A, all_of: {[B, C]}]})
	hence the 'flat' in class name
	"""

	channel: Sequence[ChannelId]
	t0_has_chan: bool = True


	@abstractmethod
	def get_first_match(self) -> dict[str, Any]:
		...


	def stock(self) -> list[dict[str, Any]]:

		return [
			self.get_first_match(),
			{
				'$project': {
					'tag': 1,
					'name': 1,
					'body': 1,
					'channel': {'$setIntersection': [self.channel, '$channel']},
					'journal': self.morph_journal('journal'),
					'ts': {chan: f'$ts.{chan}' for chan in self.channel},
				}
			}
		]


	def t0(self) -> list[dict[str, Any]]:

		ret = [
			{
				'$set': {
					'channel': self.get_channel_intersection(),
					'meta': self.conform_meta()
				}
			}
		]

		if self.t0_has_chan:
			ret.insert(0, self.get_first_match())

		return ret


	def t1(self) -> list[dict[str, Any]]:

		return [
			self.get_first_match(),
			{
				'$set': {
					'channel': self.get_channel_intersection(),
					'meta': self.conform_meta()
				}
			}
		]


	def t2(self) -> list[dict[str, Any]]:

		return [
			self.get_first_match(),
			{
				'$set': {
					'channel': self.get_channel_intersection(),
					'meta': self.conform_meta()
				}
			}
		]


	def t3(self) -> list[dict[str, Any]]:

		return [
			self.get_first_match(),
			{
				'$set': {
					'channel': self.get_channel_intersection(),
					'meta': self.conform_meta()
				}
			}
		]


	def morph_journal(self, arg: str) -> dict[str, Any]:
		"""
		If channel is an array, reduce its value to the intersection between
		the defined channels and the requested one (should work with AND or OR channel projections).
		"""

		or_list = [
			{'$eq': [chan, '$$d.channel']}
			for chan in self.channel
		]

		or_list.insert(0, {"$not": ["$$d.channel"]})

		return {
			'$filter': {
				'input': {
					'$map': {
						'input': f"${arg}",
						'as': "d",
						'in': {
							'$cond': {
								'if': {'$isArray': '$$d.channel'},
								'then': {
									'$let': {
										'vars': {
											'intersect': {'$setIntersection': [self.channel, '$$d.channel']},
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
														'then': {'$mergeObjects': ['$$d', {'channel': {"$arrayElemAt": ['$$intersect', 0]}}]}
													},
													{
														'case': {'$gt': [{'$size': '$$intersect'}, 1]},
														'then': {'$mergeObjects': ['$$d', {'channel': '$$intersect'}]}
													}
												]
											}
										}
									}
								},
								'else': {
									'$cond': {
										'if': {'$or': or_list},
										'then': '$$d',
										'else': '$$REMOVE'
									},
								},
							}
						}
					}
				},
				'as': 'd',
				'cond': {'$ne': ['$$d', None]}
			}
		}


	def get_meta_cases(self, arg: str) -> list[dict[str, Any]]:
		"""
		Note that this method is used by MongoOrView but also by MongoAndView
		because it does not make sense to require an "AND" connection for the
		channel field of meta records, the AND logical connection is used solely
		for the root 'channel' field of the ampel documents
		"""
		
		return [
			{
				'case': {
					'$or': [
						{'$eq': [f"{arg}.channel", chan]}
						for chan in self.channel
					]
				},
				'then': arg
			},
			{
				'case': {
					'$and': [
						{'$isArray': f"{arg}.channel"},
						{
							'$or': [
								{'$in': [chan, f"{arg}.channel"]}
								for chan in self.channel
							]
						}
					]
				},
				'then': {
					"$mergeObjects": [
						f"{arg}",
						{
							"channel": self.get_channel_intersection(f"{arg}.channel")
						}
					]
				}
			}
		]


	def get_channel_intersection(self, arg: str = '$channel') -> Any:

		return {
			'$let': {
				'vars': {
					'intersect': {'$setIntersection': [self.channel, arg]},
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
