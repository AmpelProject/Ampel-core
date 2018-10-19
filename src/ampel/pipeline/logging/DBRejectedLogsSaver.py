#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/logging/DBRejectedLogsSaver.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 29.09.2018
# Last Modified Date: 19.10.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import time
from logging import WARNING
from pymongo.errors import BulkWriteError
from pymongo.operations import UpdateOne
from ampel.pipeline.db.AmpelDB import AmpelDB
from ampel.pipeline.logging.DBUpdateException import DBUpdateException


#class RejectedLogs():
class DBRejectedLogsSaver():
	"""
	Class responsible for saving rejected log events (by T0 filters) 
	into the NoSQL database. This class does not inherit logging.Handler 
	but implements the method handle() so that this class can be used together 
	with RecordsBufferingHandler.forward() or copy()
	"""

	def __init__(self, channel, logger, aggregate_interval=1, flush_len=1000):
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
		self.logger = logger
		self.log_dicts = []
		self.prev_records = None
		self.run_id = None
		self.col = None 
			

	def set_run_id(self, run_id):
		""" """
		self.run_id = run_id


	def get_run_id(self):
		""" """
		return self.run_id


	def handle(self, record):
		""" 
		"""

		try: 

			extra = getattr(record, 'extra')
			
			# Same flag, date (+- 1 sec), tran_id and chans
			if (
				self.prev_records and 
				record.created - self.prev_records.created < self.aggregate_interval and 
				extra == getattr(self.prev_records, 'extra', None)
			):
	
				prev_dict = self.log_dicts[-1]
				if type(prev_dict['msg']) is not list:
					prev_dict['msg'] = [prev_dict['msg'], record.msg]
				else:
					prev_dict['msg'].append(record.msg)
	
			else:
	
				if len(self.log_dicts) > self.flush_len:
					self.flush()

				log_id = record.extra.pop('alertId')
	
				# If duplication exists between keys in extra and in standard rec,
				# the corresponding extra items will be overwritten (and thus ignored)
				d = {
					**extra, # position matters, should be first
					'_id': log_id,
					'dt': int(time.time())
				}

				if record.levelno > WARNING:
					d['runId'] = self.run_id

				if record.msg:
					d['msg'] = record.msg

				self.log_dicts.append(d)
				self.prev_records = record

		except Exception as e:
			DBUpdateException.report(self, e)
			raise e from None


	def flush(self):
		""" 
		Will raise Exception if DB issue occurs
		"""

		# No log entries
		if not self.log_dicts:
			return

		try:

			AmpelDB.get_collection(self.channel).insert_many(
				self.log_dicts, ordered=False
			)

		except BulkWriteError as bwe:

			upserts = []

			# Recovery procedure for 'already existing logs' 
			# In production, we should process alerts only once (per channel(s))
			# but during testing, reprocessing may occur. 
			# In this case, we overwrite previous rejected logs
			for err_dict in bwe.details.get('writeErrors', []):

				# 'code': 11000, 'errmsg': 'E11000 duplicate key error collection: ...
				if err_dict.get("code") == 11000:
					lid = {'_id': err_dict['op'].pop('_id')}
					del err_dict['op']['tranId']
					upserts.append(
						UpdateOne(lid, {'$set': err_dict['op']})
					)

			if len(upserts) != len(bwe.details.get('writeErrors', [])):
				DBUpdateException.report(self, bwe, bwe.details)
				raise bwe from None

			self.logger.warn("Overwriting rejected alerts logs")
			try:

				# Try again, with updates this time
				AmpelDB.get_collection(self.channel).bulk_write(
					upserts, ordered=False
				)
				self.log_dicts = []
				self.prev_records = None
				return

			except BulkWriteError as bwee:
				DBUpdateException.report(self, bwee, bwee.details)

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
