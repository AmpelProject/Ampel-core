#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/logging/DBRejectedLogsSaver.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 29.09.2018
# Last Modified Date: 29.09.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import logging, struct, os
from bson import ObjectId
from pymongo.errors import BulkWriteError
from ampel.pipeline.db.AmpelDB import AmpelDB
from ampel.pipeline.logging.DBUpdateException import DBUpdateException


class DBRejectedLogsSaver():
	"""
	Class responsible for saving rejected log events (by T0 filters) 
	into the NoSQL database.
	This class does not inherit logging.Handler but the method handle() 
	is implemented so that this class can be used together with 
	RecordsBufferingHandler.forward() or copy()
	"""

	def __init__(self, channel, aggregate_interval=1, flush_len=1000):
		""" 
		:param str channel: channel name
		:param int aggregate_interval: logs with similar attributes (log level, 
		possibly tranId & channels) are aggregated in one document instead of being split
		into several documents (spares some index RAM). *aggregate_interval* is the max interval 
		of time in seconds during which log aggregation takes place. Beyond this value, 
		attempting a database bulk_write operation.
		"""

		self.flush_len = flush_len
		self.aggregate_interval = aggregate_interval
		self.channel = channel
		self.log_dicts = []
		self.prev_records = None
		self.run_id = None
		self.col = None 
			
		# ObjectID middle: 3 bytes machine + # 2 bytes pid
		self.oid_middle = ObjectId._machine_bytes + struct.pack(">H", os.getpid() % 0xFFFF)


	def set_run_id(self, run_id):
		""" """
		self.run_id = run_id


	def handle(self, record):
		""" 
		"""

		try: 

			extra = getattr(record, 'extra')
			
			# Same flag, date (+- 1 sec), tran_id and chans
			if (
				self.prev_records and 
				record.levelno == self.prev_records.levelno and 
				record.created - self.prev_records.created < self.aggregate_interval and 
				extra == getattr(self.prev_records, 'extra', None)
			):
	
				ldict = self.log_dicts[-1]
				if type(ldict['msg']) is not list:
					ldict['msg'] = [ldict['msg']]
	
				ldict['msg'].append(record.msg)
	
			else:
	
				if len(self.log_dicts) > self.flush_len:
					self.flush()
	
				# Generate object id with log record.created as current time
				with ObjectId._inc_lock:
					oid = struct.pack(">i", int(record.created)) + \
						  self.oid_middle + struct.pack(">i", ObjectId._inc)[1:4]
					ObjectId._inc = (ObjectId._inc + 1) % 0xFFFFFF
	
				# If duplication exists between keys in extra and in standard rec,
				# the corresponding extra items will be overwritten (and thus ignored)
				self.log_dicts.append(
					{
						**extra, # position matters, should be first
						'_id': ObjectId(oid=oid),
						'lvl': record.levelno,
						'runId': self.run_id,
						'msg': record.msg,
						'channel': self.channel
					}
				)
	
				self.prev_records = record

		except Exception as e:
			DBUpdateException.report(self, e)
			raise e from None


	def flush(self):
		""" 
		Will throw Exception if DB issue occurs
		"""

		# No log entries
		if not self.log_dicts:
			return

		if self.col is None:
			self.col = AmpelDB.get_collection(self.channel)

		try:
			self.col.insert_many(self.log_dicts)
		except BulkWriteError as bwe:
			DBUpdateException.report(self, bwe, bwe.details)
			raise bwe from None
		except Exception as e:
			DBUpdateException.report(self, e)
			# If we can no longer keep track of what Ampel is doing, 
			# better raise Exception to stop processing
			raise e from None

		# Empty referenced logs entries
		self.log_dicts = []
		self.prev_records = None
