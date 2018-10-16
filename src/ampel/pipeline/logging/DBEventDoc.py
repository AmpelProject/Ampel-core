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

	def __init__(self, event_name, tier, dt=None, col_name="events"):
		""" 
		:param str event_name: event name. For example 'ap' (alertprocessor) or task name
		:param int tier: value (0,1,2,3) indicating at which ampel tier level logging is done
		:param str col_name: name of db collection to use (default 'events'). 

		Collection is retrieved using AmpelDB.get_collection()
		"""
		self.event_name = event_name
		self.tier = tier
		self.col = AmpelDB.get_collection(col_name)
		self.run_ids = []
		self.event_info = {}
		self.dt = int(datetime.utcnow().timestamp()) if dt is None else dt


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

		# Record event info into DB
		res = self.col.update_one(
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
						'dt': self.dt,
						'runId': self.run_ids[0] if len(self.run_ids) == 1 else self.run_ids,
						**self.event_info
					}
				}
			},
			upsert=True
		)

		if res.modified_count == 0 and res.upserted_id is None:
			raise ValueError(
				"Events collection update failed (%s)" % {
					'mongoUpdateResult': res.raw_result,
					'event': self.event_name
				}
			)

		self.event_info = {}
