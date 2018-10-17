#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/logging/DBLoggingHandler.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 14.12.2017
# Last Modified Date: 17.10.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import logging, struct, os
from bson import ObjectId
from pymongo.errors import BulkWriteError
from pymongo.operations import UpdateOne
from ampel.pipeline.db.AmpelDB import AmpelDB
from ampel.pipeline.logging.DBUpdateException import DBUpdateException


class DBLoggingHandler(logging.Handler):
	"""
	Custom subclass of logging.Handler responsible for 
	saving log events into the NoSQL database.
	"""

	def __init__(
		self, tier, col_name="logs", level=logging.DEBUG, aggregate_interval=1, flush_len=1000
	):
		""" 
		:param int tier: number indicating at which ampel tier level logging is done (0,1,2,3) 
		:param str col: name of db collection to use (default: 'logs' in database Ampel_var)
		:param int aggregate_interval: logs with similar attributes (log level, 
		possibly tranId & channels) are aggregated in one document instead of being split
		into several documents (spares some index RAM). *aggregate_interval* is the max interval 
		of time in seconds during which log aggregation takes place. Beyond this value, 
		a new log document is created no matter what. It thus impacts the logging time granularity.
		:param int flush_len: How many log documents should be kept in memory before
		attempting a database bulk_write operation.
		"""

		# required by parent
		self.filters = []
		self.lock = None
		self._name = None

		# Own stuff
		self.flush_len = flush_len
		self.aggregate_interval = aggregate_interval
		self.tier = tier
		self.log_dicts = []
		self.prev_records = None
		self.run_id = self.new_run_id()
		self.headers = []

		# Get reference to pymongo collection
		self.col = AmpelDB.get_collection(col_name)
			
		# Set loggingHandler properties
		self.setLevel(level)

		# ObjectID middle: 3 bytes machine + # 2 bytes pid
		self.oid_middle = ObjectId._machine_bytes + struct.pack(">H", os.getpid() % 0xFFFF)


	def new_run_id(self):
		""" 
		runId is a global (ever increasing) counter stored in the DB
		used to tie log entries from the same process with each other
		"""
		return AmpelDB.get_collection('counter').find_one_and_update(
			{'_id': 'current_run_id'},
			{'$inc': {'value': 1}},
			new=True, upsert=True
		).get('value')


	def add_headers(self, dicts):
		"""
		:param list dicts: list of dict instances
		"""
		if len(dicts) > self.flush_len:
			# We could do something about that.. later if need be
			raise ValueError("Too many logrecord headers")

		self.headers = dicts


	def emit(self, record):
		""" 
		"""

		extra = getattr(record, 'extra', None)

		try: 

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
						  self.oid_middle + \
						  struct.pack(">i", ObjectId._inc)[1:4]
					ObjectId._inc = (ObjectId._inc + 1) % 0xFFFFFF
	
				if extra:
					# If duplication exists between keys in extra and in standard rec,
					# the corresponding extra items will be overwritten (and thus ignored)
					ldict = {
						**extra, # position matters, should be first
						'_id': ObjectId(oid=oid),
						'tier': self.tier,
						'runId': self.run_id,
						'lvl': record.levelno,
						'msg': record.msg
					}
				else:
					ldict = {
						'_id': ObjectId(oid=oid),
						'tier': self.tier,
						'runId': self.run_id,
						'lvl': record.levelno,
						'msg': record.msg
					}
	
				if record.levelno > logging.WARNING:
					ldict['filename'] = record.filename,
					ldict['lineno'] = record.lineno,
					ldict['funcName'] = record.funcName,
			
				self.log_dicts.append(ldict)
				self.prev_records = record

		except Exception as e:
			DBUpdateException.report(self, e)
			raise e from None


	def get_run_id(self):
		""" """
		return self.run_id


	def add_run_id(self, run_id):
		""" 
		:param int run_id: run ID
		"""
		if type(self.run_id) is int:
			self.run_id = [self.run_id, run_id]
		else:
			self.run_id.append(run_id)


	def fork(self):
		""" 
		:returns: new instance of DBLoggingHandler
		"""
		# New instance of DBLoggingHandler with same parameters
		dblh = DBLoggingHandler(
			self.tier, self.col.name, self.level, self.aggregate_interval, self.flush_len
		)

		# Add runId from new instance to current instance
		self.add_run_id(
			dblh.get_run_id()
		)

		# Vice-versa
		dblh.add_run_id(
			self.get_run_id()
		)

		if not hasattr(self, "child_handlers"):
			self.child_handlers = []

		self.child_handlers.append(dblh)

		return dblh


	def flush_all(self):
		"""
		Flushes also child handlers if any
		"""
		if hasattr(self, "child_handlers"):
			for handler in self.child_handlers:
				handler.flush()
		self.flush()


	def purge(self):
		""" 
		"""
		self.log_dicts=[]
		self.prev_records = None


	def flush(self):
		""" 
		Will throw Exception if DB issue occur
		"""

		# No log entries
		if not self.log_dicts:
			return

		try:
			if self.headers:
				self.col.bulk_write(
					[
						UpdateOne(
							{'_id': rec['_id']},
							{
								'$setOnInsert': rec,
								'$addToSet': {
									'runId': self.run_id
								} 
							},
							upsert=True
						)
						for rec in self.headers
					]
				)
				self.headers = None

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
