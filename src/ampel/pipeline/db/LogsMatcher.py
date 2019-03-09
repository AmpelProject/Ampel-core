#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/db/LogsMatcher.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 29.11.2018
# Last Modified Date: 09.03.2019
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from ampel.pipeline.logging.AmpelLogger import AmpelLogger
from ampel.pipeline.db.AmpelDB import AmpelDB

class LogsMatcher:
	"""
	"""

	def __init__(self):
		"""
		"""
		self.match = {}


	def get_match_criteria(self):
		""" 
		:returns: dict
		"""
		return self.match


	def set_flag(self, arg):
		""" 
		The mongodb operator "$bitsAllSet" will be used.

		:param LogRecordFlag arg:
		"""
		self.match['flag'] = {"$bitsAllSet": arg}


	def set_channels(self, channels, compact_logs=True):
		""" 
		:type channels: str, list[str]
		"""
		self.channels = channels
		if compact_logs:
			self.match['$or'] = [
				{'channels': channels},
				{'msg.channels': channels}
			]
		else:
			self.match['channels'] = channels
			

	def set_tran_ids(self, tran_ids):
		""" 
		:type tran_ids: int, List[int]
		"""
		self.match['tranId'] = tran_ids


	def set_run_ids(self, run_ids):
		""" 
		:type run_id: int, List[int]
		"""
		self.match['runId'] = run_ids


	def set_alert_ids(self, arg):
		""" """
		self.match['alertId'] = arg


	def set_after(self, dt):
		""" 
		Note: time operation is greater than / *equals*
		:param datetime dt:
		""" 
		self._set_time_constraint(dt, '$gte')


	def set_before(self, dt):
		""" 
		Note: time operation is before than / *equals*
		:param datetime dt:
		""" 
		self._set_time_constraint(dt, '$lte')


	def _set_time_constraint(self, dt, op):
		""" 
		Note: time operation is greater than / *equals*
		:param datetime dt:
		""" 
		from bson.objectid import ObjectId
		if "_id" not in self.match:
			self.match["_id"] = {}
			
		self.match["_id"][op] = ObjectId.from_datetime(dt)


