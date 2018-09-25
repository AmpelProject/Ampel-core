#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/logging/DBLoggingHandler.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 14.12.2017
# Last Modified Date: 25.09.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import logging
from pymongo import MongoClient
from pymongo.errors import BulkWriteError
from bson import ObjectId
from time import time
from datetime import datetime

from ampel.pipeline.db.AmpelDB import AmpelDB
from ampel.pipeline.config.AmpelConfig import AmpelConfig

class DBLoggingHandler(logging.Handler):
	"""
	Custom subclass of logging.Handler responsible for 
	logging log events into the NoSQL database.
	"""

	def __init__(self, tier, col="jobs", aggregate_interval=1, flush_len=1000):
		""" 
		:param int tier: number indicating at which ampel tier level logging is done (0,1,2,3) 
		:param str col: name of db collection to use
		-> If None default 'stats' collection from AmpelDB will be used
		-> otherwise, the provided db_col will be used
		:param int aggregate_interval: logs with equals attributes 
		(log level, possibly tranId & alertId) are put together in one document instead
		of being splitted in one document per log entry (spares some index RAM).
		aggregate_interval is the max interval in seconds during which the log aggregation
		takes place. Beyond this value, a new log document is created no matter what.
		Default value is 1.
		:param int flush_lean: How many log documents should be kept in memory before
		attempting a DB bulk_write operation.
		"""
		self.flush_len = flush_len
		self.flush_force = flush_len + 50
		self.aggregate_interval = aggregate_interval
		self.tran_id = None
		self.channels = None
		self.tier = tier
		self.records = []
		self.filters = []  # required when extending logging.Handler
		self.lock = None   # required when extending logging.Handler
		self._name = None
		self.last_rec = None

		# runId is a global (ever increasing) counter stored in the DB
		# used to tie log entries from the same process with each other
		self.run_id = AmpelDB.get_collection('counter').find_one_and_update(
			{'_id': 'current_run_id'},
			{'$inc': {'value': 1}},
			new=True, upsert=True
		).get('value')

		# Get reference to pymongo collection
		self.col = AmpelDB.get_collection(col)

		# Set loggingHandler properties
		self.setLevel(logging.DEBUG)
		self.setFormatter(logging.Formatter('%(message)s'))


	def set_channels(self, arg):
		""" """
		self.channels = arg


	def unset_channels(self):
		""" """
		self.channels = None


	def set_tran_id(self, arg):
		""" """
		self.tran_id = arg


	def unset_tran_id(self):
		""" """
		self.tran_id = None


#	@abstractmethod
#	def can_aggregate(self, record):
#
#		return (
#			self.last_rec and 
#			record.levelno == self.last_rec['lvl'] and 
#			time() - self.last_rec['_id'].generation_time.timestamp() < self.aggregate_interval and 
#			self.tran_id == self.last_rec.get('tranId') and
#			self.channels == self.last_rec.get('channels')
#		)


	def emit(self, record):
		""" """

		# Same flag, date (+- 1 sec), tran_id and chans
		if (
			self.last_rec and 
			record.levelno == self.last_rec['lvl'] and 
			time() - self.last_rec['_id'].generation_time.timestamp() < self.aggregate_interval and 
			self.tran_id == self.last_rec.get('tranId') and
			self.channels == self.last_rec.get('channels')
		):

			rec = self.last_rec
			if type(rec['msg']) is not list:
				rec['msg'] = [rec['msg']]

			rec['msg'].append(record.msg)

		else:

			if len(self.records) > self.flush_len:
				self.flush()

			rec = {
				'_id': ObjectId(),
				'tier': self.tier,
				'runId': self.run_id,
				'lvl': record.levelno,
				'msg': self.format(record)
			}

			if record.levelno > logging.INFO:
				rec['filename'] = record.filename,
				rec['lineno'] = record.lineno,
				rec['funcName'] = record.funcName,
		
			if self.tran_id:
				rec['tranId'] = self.tran_id

			if self.channels:
				rec['channels'] = self.channels

			self.records.append(rec)
			self.last_rec = rec

		if len(self.records) > self.flush_force:
			self.flush()


	def get_run_id(self):
		""" """
		return self.run_id


	def remove_log_entries(self):
		""" """

		del_result = self.col.delete_many(
			{'runId': self.run_id}
		)


	def flush(self):
		""" 
		Will throw Exception if DB issue exists
		"""

		# No log entries
		if not self.records:
			return

		# Perform DB operation
		try:
			res = self.col.insert_many(self.records)
		except BulkWriteError as bwe:
			logging.error(bwe.details)
			return

		# Update result check
		if len(res.inserted_ids) != len(self.records):

			# Print log stack using std logging 
			logging.error("DB log flushing error with following log stack:")
			logging.error("###############################################")
			for el in self.records:
				if el['_id'] in res.inserted_ids:
					logging.info(el)
				else:
					logging.error(el)
			logging.error("###############################################")

			# If we can no longer keep track of what Ampel is doing, 
			# better raise Exception to stop processing
			raise ValueError(
				"Log flushing error: %i records present but %i were inserted into db" % (
					len(self.records), len(res.inserted_ids),
				)
			)

		# Empty referenced logs entries
		self.records = []
		self.last_rec = None
