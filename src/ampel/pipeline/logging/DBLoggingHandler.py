#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/logging/DBLoggingHandler.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 14.12.2017
# Last Modified Date: 12.05.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import logging
from ampel.flags.LogRecordFlags import LogRecordFlags
from ampel.flags.JobFlags import JobFlags


class DBLoggingHandler(logging.Handler):
	"""
		Custom subclass of logging.Handler responsible for 
		logging log events into the NoSQL database.
		An instance of ampel.pipeline.logging.DBJobReporter is required as parameter.
		Each database log entry contains a global flag (ampel.flags.LogRecordFlags)
		which includes the log severity level.
	"""
	severity_map = {
		10: LogRecordFlags.DEBUG,
		20: LogRecordFlags.INFO,
		30: LogRecordFlags.WARNING,
		40: LogRecordFlags.ERROR,
		50: LogRecordFlags.CRITICAL
	}

	def __init__(self, db_job_reporter, previous_logs=None, flush_len=1000):
		""" """

		if db_job_reporter.get_job_id() is None:
			raise ValueError("DBJobReporter has None JobId")

		self.db_job_reporter = db_job_reporter
		self.global_flags = LogRecordFlags(0)
		self.temp_flags = LogRecordFlags(0)
		self.flush_len = flush_len
		self.flush_force = flush_len + 50
		self.compoundId = None
		self.tranId = None
		self.channels = None
		self.records = []
		self.filters = []  # required when extending logging.Handler
		self.lock = None   # required when extending logging.Handler
		self._name = None
		self.last_rec = {'fl': -1}

		self.setLevel(logging.DEBUG)
		self.setFormatter(logging.Formatter('%(message)s'))

		if previous_logs is not None:
			self.prepend_logs(previous_logs)

	def set_db_job_reporter(self, arg):
		""" """
		self.db_job_reporter = arg

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

	def set_compoundId(self, arg):
		""" """
		self.compoundId = arg

	def unset_compoundId(self):
		""" """
		self.compoundId = None

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

			if self.compoundId is not None:
				rec['cp'] = self.compoundId

			rec['ms'] = self.format(record) if self.channels is None else {
                "m" : self.format(record),
                "ch" : self.channels
            }
		
			self.last_rec = rec
			self.records.append(rec)


		if record.levelno == 40:
			self.db_job_reporter.add_flags(JobFlags.HAS_ERROR)
		elif record.levelno == 50:
			self.db_job_reporter.add_flags(JobFlags.HAS_CRITICAL)

		if len(self.records) > self.flush_force:
			self.flush()


	def prepend_logs(self, logs):
		"""
		"""
		if not type(logs) is list:
			logs = [logs]
			
		self.records[0:0] = logs


	def flush(self):
		""" """
		self.db_job_reporter.push_logs(self.records)
		self.records = []
