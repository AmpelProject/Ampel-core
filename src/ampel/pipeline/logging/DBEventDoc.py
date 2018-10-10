#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/logging/DBEventDoc.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 26.09.2018
# Last Modified Date: 10.10.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from datetime import datetime
from ampel.pipeline.db.AmpelDB import AmpelDB

class DBEventDoc():
	"""
	"""

	def __init__(self, name, tier, col="events"):
		""" 
		:param str name: event name. For example 'ap' (alertprocessor) or task name
		:param int tier: value (0,1,2,3) indicating at which ampel tier level logging is done
		:param str col: name of db collection to use (default 'events'). 

		Collection is retrieved using AmpelDB.get_collection()
		"""
		self.event_name = name
		self.tier = tier
		self.col = AmpelDB.get_collection(col)
		self.run_ids = []
		self.event_info = {}


	def set_event_info(self, event_info):
		""" 
		:param dict event_info:
		:returns: None
		"""
		self.event_info = event_info


	def add_run_id(self, run_id):
		"""
		:param int run_id: 
		:returns: None
		"""
		self.run_ids.append(run_id)


	def publish(self):
		"""
		This action flushes the logs as well.

		:returns: None
		"""

		self.col.update_one(
			{
				'_id': int(
					datetime.today().strftime('%Y%m%d')
				)
			},
			{
				'$push': {
					'events': {
						'event': self.event_name,
						'tier': self.tier,
						'dt': datetime.utcnow().timestamp(),
						'runId': self.run_ids[0] if len(self.run_ids) == 1 else self.run_ids,
						**self.event_info
					}
				}
			},
			upsert=True
		)

		self.event_info = {}
