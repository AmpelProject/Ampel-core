#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/mongo/view/MongoOneView.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 26.03.2021
# Last Modified Date: 28.03.2021
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import List, Any, Dict
from ampel.type import ChannelId
from ampel.mongo.view.AbsMongoView import AbsMongoView

class MongoOneView(AbsMongoView):

	channel: ChannelId

	def stock(self) -> List[Dict[str, Any]]:

		return [
			{'$match': {'channel': self.channel}},
			{
				'$addFields': {
					'created': f"$created.{self.channel}",
					'modified': f"$modified.{self.channel}",
					'journal': self.filter_journal()
				}
			},
			{'$project': {'channel': 0, 'journal.channel': 0}},
		]


	def t0(self) -> List[Dict[str, Any]]:
		return []


	def t1(self) -> List[Dict[str, Any]]:

		return [
			{'$match': {'channel': self.channel}},
			{'$project': {'channel': 0}}
		]


	def t2(self) -> List[Dict[str, Any]]:

		return [
			{'$match': {'channel': self.channel}},
			{'$addFields': {'journal': self.filter_journal()}},
			{'$project': {'channel': 0, 'journal.channel': 0}},
		]


	def t3(self) -> List[Dict[str, Any]]:

		return [
			{'$match': {'channel': self.channel}},
			{'$project': {'channel': 0}},
		]


	def filter_journal(self) -> Dict[str, Any]:

		return {
			'$filter': {
				'input': "$journal",
				'as': "journal",
				'cond': {
					'$or': [
						{'$eq': ['$$journal.channel', self.channel]},
						{
							'$and': [
								{'$isArray': '$$journal.channel'},
								{'$in': [self.channel, '$$journal.channel']}
							]
						}
					]
				}
			}
		}
