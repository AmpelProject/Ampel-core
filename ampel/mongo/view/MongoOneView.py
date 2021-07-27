#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/mongo/view/MongoOneView.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 26.03.2021
# Last Modified Date: 21.07.2021
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import List, Any, Dict
from ampel.types import ChannelId
from ampel.mongo.view.AbsMongoView import AbsMongoView

class MongoOneView(AbsMongoView):

	channel: ChannelId
	t0_has_chan: bool = True

	def stock(self) -> List[Dict[str, Any]]:

		return [
			{'$match': {'channel': self.channel}},
			{
				'$addFields': {
					'ts': f"$ts.{self.channel}",
					'journal': self.filter_seq('journal')
				}
			},
			{'$project': {'channel': 0, 'journal.channel': 0}},
		]


	def t0(self) -> List[Dict[str, Any]]:
		if self.t0_has_chan:
			return [
				{'$match': {'channel': self.channel}},
				{'$addFields': {'meta': self.filter_seq('meta')}},
				{'$project': {'channel': 0, 'meta.channel': 0}}
			]
		return []

	def t1(self) -> List[Dict[str, Any]]:

		return [
			{'$match': {'channel': self.channel}},
			{'$addFields': {'meta': self.filter_seq('meta')}},
			{'$project': {'channel': 0, 'meta.channel': 0}}
		]


	def t2(self) -> List[Dict[str, Any]]:

		return [
			{'$match': {'channel': self.channel}},
			{'$addFields': {'meta': self.filter_seq('meta')}},
			{'$project': {'channel': 0, 'meta.channel': 0}},
		]


	def t3(self) -> List[Dict[str, Any]]:

		return [
			{'$match': {'channel': self.channel}},
			{'$addFields': {'meta': self.filter_seq('meta')}},
			{'$project': {'channel': 0, 'meta.channel': 0}}
		]


	def filter_seq(self, arg: str) -> Dict[str, Any]:

		return {
			'$filter': {
				'input': f"${arg}",
				'as': arg,
				'cond': {
					'$or': [
						{"$not": [f"$${arg}.channel"]},
						{'$eq': [f"$${arg}.channel", self.channel]},
						{
							'$and': [
								{'$isArray': f"$${arg}.channel"},
								{'$in': [self.channel, f"$${arg}.channel"]}
							]
						}
					]
				}
			}
		}
