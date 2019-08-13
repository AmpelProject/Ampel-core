#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/logging/DBEventDoc.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 26.09.2018
# Last Modified Date: 11.11.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from time import time, strftime
from ampel.pipeline.db.AmpelDB import AmpelDB

class DBEventDoc():
	"""
	"""

	default_doc_id = '%Y%m%d'

	def __init__(self, event_name, tier, dt=None, col_name="events", doc_id=None):
		""" 
		:param str event_name: event name. For example 'ap' (alertprocessor) or task name
		:param int tier: value (0,1,2,3) indicating at which ampel tier level logging is done
		:param str col_name: name of db collection to use (default 'events'). 
		:param str doc_id: optional string that will be provided as argument to method `strftime` (default: '%Y%m%d')
		:param int dt: timestamp

		Collection is retrieved using AmpelDB.get_collection()
		"""
		self.event_name = event_name
		self.tier = tier
		self.col = AmpelDB.get_collection(col_name)
		self.run_ids = []
		self.doc_id = doc_id if doc_id else DBEventDoc.default_doc_id
		self.event_info = {}
		self.dt = int(time()) if dt is None else int(dt)
		self.duration = 0


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


	def add_duration(self, seconds):
		"""
		:param seconds:
		:type seconds: int, float
		"""
		self.duration += seconds


	def publish(self):
		"""
		:returns: None
		"""

		# Record event info into DB
		res = self.col.update_one(
			{
				'_id': int(strftime(self.doc_id))
			},
			{
				'$push': {
					'events': {
						'event': self.event_name,
						'tier': self.tier,
						'dt': self.dt,
						'duration': int(time()-self.dt) if self.duration == 0 else int(self.duration),
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
