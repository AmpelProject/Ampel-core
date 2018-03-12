#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/logging/DBLoggingHandler.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 14.12.2017
# Last Modified Date: 11.03.2018
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

	def __init__(self, db_job_reporter, previous_logs=None, flush_len=500):
		""" """
		self.db_job_reporter = db_job_reporter
		self.global_flags = LogRecordFlags(0)
		self.temp_flags = LogRecordFlags(0)
		self.flush_len = flush_len
		self.compoundId = None
		self.tranId = None
		self.records = []
		self.filters = []  # required when extending logging.Handler
		self.lock = None   # required when extending logging.Handler
		self._name = None

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

	def set_tranId(self, arg):
		""" """
		self.tranId = arg

	def unset_tranId(self):
		""" """
		self.tranId = None

	def emit(self, record):
		""" """
		rec = {
			'date': int(record.created),
			'flags': (self.global_flags | self.temp_flags | DBLoggingHandler.severity_map[record.levelno]).value,
			#'filename': record.filename,
			#'lineno': record.lineno,
			#'funcName': record.funcName,
			'msg': self.format(record)
		}

		if self.tranId is not None:
			rec['tranId'] = self.tranId

		if self.compoundId is not None:
			rec['compoundId'] = self.compoundId

		self.records.append(rec)

		if record.levelno == 40:
			self.db_job_reporter.add_flags(JobFlags.HAS_ERROR)
		elif record.levelno == 50:
			self.db_job_reporter.add_flags(JobFlags.HAS_CRITICAL)

		if len(self.records) > self.flush_len and self.db_job_reporter.getJobId() is not None:
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
