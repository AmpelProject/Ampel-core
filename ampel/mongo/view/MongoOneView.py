#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/mongo/view/MongoOneView.py
# License:             BSD-3-Clause
# Author:              valery brinnel <firstname.lastname@gmail.com>
# Date:                26.03.2021
# Last Modified Date:  07.10.2021
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

from typing import Any
from ampel.types import ChannelId
from ampel.mongo.view.AbsMongoView import AbsMongoView

class MongoOneView(AbsMongoView):

	channel: ChannelId
	t0_has_chan: bool = True

	def stock(self) -> list[dict[str, Any]]:

		return [
			{'$match': {'channel': self.channel}},
			{
				'$addFields': {
					'ts': f"$ts.{self.channel}",
					'journal': self.filter_journal('journal')
				}
			},
			{'$project': {'channel': 0, 'journal.channel': 0}},
		]


	def t0(self) -> list[dict[str, Any]]:
		if self.t0_has_chan:
			return [
				{'$match': {'channel': self.channel}},
				{'$addFields': {'meta': self.conform_meta()}},
				{'$project': {'channel': 0, 'meta.activity.channel': 0}}
			]
		return []

	def t1(self) -> list[dict[str, Any]]:

		return [
			{'$match': {'channel': self.channel}},
			{'$addFields': {'meta': self.conform_meta()}},
			{'$project': {'channel': 0, 'meta.activity.channel': 0}}
		]


	def t2(self) -> list[dict[str, Any]]:

		return [
			{'$match': {'channel': self.channel}},
			{'$addFields': {'meta': self.conform_meta()}},
			{'$project': {'channel': 0, 'meta.activity.channel': 0}}
		]


	def t3(self) -> list[dict[str, Any]]:

		return [
			{'$match': {'channel': self.channel}},
			{'$addFields': {'meta': self.conform_meta()}},
			{'$project': {'channel': 0, 'meta.activity.channel': 0}}
		]


	def filter_journal(self, arg: str) -> dict[str, Any]:

		return {
			'$filter': {
				'input': f"${arg}",
				'as': "out",
				'cond': {
					'$or': [
						{"$not": ["$$out.channel"]},
						{'$eq': ["$$out.channel", self.channel]},
						{
							'$and': [
								{'$isArray': "$$out.channel"},
								{'$in': [self.channel, "$$out.channel"]}
							]
						}
					]
				}
			}
		}


	def get_meta_cases(self, arg: str) -> list[dict[str, Any]]:

		return [
			{'case': {'$eq': [self.channel, f"{arg}.channel"]}, 'then': arg},
			{
				'case': {
					'$and': [
						{'$isArray': f"{arg}.channel"},
						{'$in': [self.channel, f"{arg}.channel"]}
					]
				},
				'then': arg
			}
		]
