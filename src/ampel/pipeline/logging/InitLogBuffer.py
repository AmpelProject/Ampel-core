#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/logging/InitLogBuffer.py
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 21.01.2018
# Last Modified Date: 21.01.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>
import logging
from ampel.flags.LogRecordFlags import LogRecordFlags
from ampel.flags.JobFlags import JobFlags
from ampel.pipeline.logging.DBLoggingHandler import DBLoggingHandler


class InitLogBuffer(logging.Handler):
	"""
	"""

	def __init__(self, global_flags):
		""" 
		"""
		self.filters = []  # required when extending logging.Handler
		self.lock = None   # required when extending logging.Handler
		self._name = None
		self.records = []
		self.global_flags = global_flags

		self.setFormatter(
			logging.Formatter('%(message)s')
		)

		self.setLevel(logging.DEBUG)


	def get_logs(self):
		return self.records


	def emit(self, record):
		""" 
		"""
		rec = {
			'date': int(record.created),
			'flags': (self.global_flags | DBLoggingHandler.severity_map[record.levelno]).value,
			'msg': self.format(record)
		}

		self.records.append(rec)
