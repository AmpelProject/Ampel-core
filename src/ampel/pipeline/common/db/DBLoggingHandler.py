#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/common/db/DBLoggingHandler.py
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 14.12.2017
# Last Modified Date: 24.12.2017
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>
from ampel.flags.LogRecordFlags import LogRecordFlags
from ampel.flags.JobFlags import JobFlags
import logging

class DBLoggingHandler(logging.Handler):
	"""
		Custom subclass of logging.Handler responsible for 
		logging log events into the NoSQL database.
		An instance of common.logging.DBJobUpdater is required as parameter.
		Each database log entry contains a global flag (common.flags.LogRecordFlags)
		which includes the log severity level.
	"""
	severity_map = {
		10: LogRecordFlags.DEBUG,
		20: LogRecordFlags.INFO,
		30: LogRecordFlags.WARNING,
		40: LogRecordFlags.ERROR,
		50: LogRecordFlags.CRITICAL
	}

	def __init__(self, db_job_updater, flush_len=50):
		""" """
		self.db_job_updater = db_job_updater
		self.global_flags = LogRecordFlags(0)
		self.temp_flags = LogRecordFlags(0)
		self.flush_len = flush_len
		self.compoundId = None
		self.tranId = None
		self.records = []
		self.filters = []  # required when extending logging.Handler
		self.lock = None   # required when extending logging.Handler

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
			'filename': record.filename,
			'lineno': record.lineno,
			'funcName': record.funcName,
			'msg': self.format(record)
		}

		if self.tranId is not None:
			rec['tranId'] = self.tranId

		if self.compoundId is not None:
			rec['compoundId'] = self.compoundId

		self.records.append(rec)

		if (record.levelno == 40):
			self.db_job_updater.add_flags(JobFlags.HAS_ERROR)
		elif (record.levelno == 50):
			self.db_job_updater.add_flags(JobFlags.HAS_CRITICAL)

		if (len(self.records) > self.flush_len):
			self.flush()

	def flush(self):
		""" """
		self.db_job_updater.push_logs(self.records)
		self.records = []
