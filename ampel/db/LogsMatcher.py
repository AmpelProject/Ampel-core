#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/db/LogsMatcher.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 29.11.2018
# Last Modified Date: 15.02.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from datetime import datetime

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

		:param LogFlag arg:
		"""
		self.match['flag'] = {"$bitsAllSet": arg}
		return self


	def set_channels(self, channels, compact_logs=True):
		"""
		:type channels: str, list[str]
		"""
		self.channels = channels
		if compact_logs:
			self.match['$or'] = [
				{'channel': channels},
				{'msg.channel': channels}
			]
		else:
			self.match['channel'] = channels
		return self


	def set_stock_ids(self, stock_ids):
		"""
		:type tran_ids: int, List[int]
		"""
		self.match['stock'] = stock_ids
		return self


	def set_run_ids(self, run_ids):
		"""
		:type run_id: int, List[int]
		"""
		self.match['run'] = run_ids
		return self


	def set_alert_ids(self, arg):
		""" """
		self.match['alert'] = arg
		return self


	def set_custom(self, key, value):
		""" """
		self.match[key] = value
		return self


	def set_after(self, dt):
		"""
		Note: time operation is greater than / *equals*
		:param dt: date-time
		:type dt: either datetime object or string (ex: '2018-06-29 08:15:27')
		"""

		self._set_time_constraint(dt, '$gte')
		return self


	def set_before(self, dt):
		"""
		Note: time operation is before than / *equals*
		:param dt: date-time
		:type dt: either datetime object or string (ex: '2018-06-29 08:15:27')
		"""
		self._set_time_constraint(dt, '$lte')
		return self


	def _set_time_constraint(self, dt, op):
		"""
		Note: time operation is greater than / *equals*
		:param dt: date-time
		:type dt: either datetime object or string (ex: '2018-06-29 08:15:27.243860')
		"""

		if isinstance(dt, datetime):
			pass
		elif isinstance(dt, str):
			dt = datetime.strptime(dt, '%Y-%m-%d %H:%M:%S')
		else:
			raise ValueError()

		from bson.objectid import ObjectId
		if "_id" not in self.match:
			self.match["_id"] = {}

		self.match["_id"][op] = ObjectId.from_datetime(dt)
		return self
