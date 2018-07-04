#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/logging/DBLoggingHandler.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 14.12.2017
# Last Modified Date: 04.07.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import logging, time
from pymongo import MongoClient

from ampel.pipeline.db.AmpelDB import AmpelDB
from ampel.core.flags.LogRecordFlags import LogRecordFlags
from ampel.pipeline.config.AmpelConfig import AmpelConfig

class DBLoggingHandler(logging.Handler):
	"""
		Custom subclass of logging.Handler responsible for 
		logging log events into the NoSQL database.
		Each database log entry contains a global flag (ampel.core.flags.LogRecordFlags)
		which includes the log severity level.
	"""
	severity_map = {
		10: LogRecordFlags.DEBUG,
		20: LogRecordFlags.INFO,
		30: LogRecordFlags.WARNING,
		40: LogRecordFlags.ERROR,
		50: LogRecordFlags.CRITICAL
	}

	def __init__(self, tier, info=None, logs_collection=None, previous_logs=None, flush_len=1000):
		""" 
		'tier': integer (0,1,2,3) indicating at which ampel tier level loggin is done
		'info': optional dict instance to be include in the root doc 
		'logs_collection': None or instance of pymongo.collection.Collection 
			-> If None default 'stats' collection from AmpelDB will be used
			-> otherwise, the provided logs_collection will be used
		"""
		self.global_flags = LogRecordFlags(0)
		self.temp_flags = LogRecordFlags(0)
		self.flush_len = flush_len
		self.flush_force = flush_len + 50
		self.tranId = None
		self.channels = None
		self.records = []
		self.filters = []  # required when extending logging.Handler
		self.lock = None   # required when extending logging.Handler
		self._name = None
		self.has_error = False
		self.last_rec = {'fl': -1}
		self.setLevel(logging.DEBUG)
		self.setFormatter(logging.Formatter('%(message)s'))

		# Get reference to pymongo 'jobs' collection
		self.col = AmpelDB.get_collection('jobs') if logs_collection is None else logs_collection

		# Will raise Exception if DB connectivity issue exist
		self.log_id = self.col.insert_one({"tier": tier, "info": info}).inserted_id

		if previous_logs is not None:
			self.prepend_logs(previous_logs)

	def set_global_flags(self, arg):
		""" """
		self.global_flags |= arg

	def remove_global_flags(self, arg):
		""" """
		self.global_flags &= ~arg

	def set_temp_flags(self, arg):
		""" """
		self.temp_flags |= arg

	def unset_temp_flags(self, arg):
		""" """
		self.temp_flags &= ~arg

	def set_channels(self, arg):
		""" """
		self.channels = arg

	def unset_channels(self):
		""" """
		self.channels = None

	def set_tranId(self, arg):
		""" """
		self.tranId = arg

	def unset_tranId(self):
		""" """
		self.tranId = None

	def emit(self, record):
		""" """

		# TODO (note to self): Comment code
		rec_dt = int(record.created)
		rec_flag = (self.global_flags | self.temp_flags | DBLoggingHandler.severity_map[record.levelno]).value

		# Same flag and date (+- 1 sec)
		if (
			rec_flag == self.last_rec['fl'] and 
			(rec_dt == self.last_rec['dt'] or rec_dt - 1 == self.last_rec['dt']) and 
			(
				(self.tranId is None and 'tr' not in self.last_rec) or
				(self.tranId is not None and 'tr' in self.last_rec and self.tranId == self.last_rec['tr'])
			)
		):

			rec = self.last_rec
			if type(rec['ms']) is not list:
				rec['ms'] = [rec['ms']]

			if self.channels is not None:
				last_log = rec['ms'][-1]
				if type(last_log) is dict and 'ch' in last_log and last_log['ch'] == self.channels:
					if type(last_log['m']) is not list:
						last_log['m'] = [last_log['m']]
					last_log['m'].append(self.format(record))
				else:
					rec['ms'].append(
						{
 							"m" : self.format(record),
							"ch" : self.channels
						}
					)
			else: 
				rec['ms'].append(self.format(record))
		else:

			if len(self.records) > self.flush_len:
				self.flush()

			rec = {
				'dt': rec_dt,
				'fl': rec_flag
				#'filename': record.filename,
				#'lineno': record.lineno,
				#'funcName': record.funcName,
			}
		
			if self.tranId is not None:
				rec['tr'] = self.tranId

			rec['ms'] = self.format(record) if self.channels is None else {
                "m" : self.format(record),
                "ch" : self.channels
            }
		
			self.last_rec = rec
			self.records.append(rec)

		if record.levelno == 40 or record.levelno == 50:
			self.has_error = True

		if len(self.records) > self.flush_force:
			self.flush()


	def prepend_logs(self, logs):
		"""
		"""
		if not type(logs) is list:
			logs = [logs]
			
		self.records[0:0] = logs


	def get_log_id(self):
		""" 
		get ObjectId of document caintaing the logs
		"""
		return self.log_id


	def remove_log_entry(self):
		""" """

		del_result = self.col.delete_one({'_id': self.log_id})

		# Update result check
		if del_result.deleted_count != 1 or del_result.acknowledged is False:
			raise ValueError("Deletion failed: %s" % del_result.raw_result)


	def flush(self):
		""" 
		Will throw Exception if DB issue exists
		"""

		# No log entries
		if not self.records:
			return

		# Perform DB operation
		update_result = self.col.update_one(
			{'_id': self.log_id},
			{
				'$push': {
					'records': { 
						'$each': self.records
					}
				},
				'$set': {
					'duration': int(time.time() - self.log_id.generation_time.timestamp()),
					'hasError': self.has_error
				}
			},
			upsert=True
		)

		# Update result check
		if update_result.modified_count != 1 or update_result.acknowledged is False:
			raise ValueError("Modification failed: %s" % update_result.raw_result)

		# Empty referenced logs entries
		self.records = []
