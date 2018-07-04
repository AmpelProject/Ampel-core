#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/logging/InitLogBuffer.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 21.01.2018
# Last Modified Date: 04.07.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import logging
from ampel.core.flags.LogRecordFlags import LogRecordFlags
from ampel.pipeline.logging.DBLoggingHandler import DBLoggingHandler


class InitLogBuffer(logging.Handler):
	"""
	"""

	def __init__(self, global_flags=None):
		""" 
		"""
		self.filters = []  # required when extending logging.Handler
		self.lock = None   # required when extending logging.Handler
		self._name = None
		self.records = []
		self.global_flags = global_flags if global_flags is not None else LogRecordFlags.ILB
		self.last_rec = {'fl': -1}

		self.setFormatter(
			logging.Formatter('%(message)s')
		)

		self.setLevel(logging.DEBUG)


	def get_logs(self):
		return self.records


	def emit(self, record):
		""" 
		"""

		rec_dt = int(record.created)
		rec_flag = (self.global_flags | DBLoggingHandler.severity_map[record.levelno]).value

		# Same flag and date (+- 1 sec)
		if (
			rec_flag == self.last_rec['fl'] and 
			(rec_dt == self.last_rec['dt'] or rec_dt - 1 == self.last_rec['dt'])
		):

			rec = self.last_rec
			if type(rec['ms']) is not list:
				rec['ms'] = [rec['ms']]

			rec['ms'].append(self.format(record))

		else:

			rec = {
				'dt': int(record.created),
				'fl': (self.global_flags | DBLoggingHandler.severity_map[record.levelno]).value,
				'ms': self.format(record)
			}
			self.last_rec = rec
			self.records.append(rec)
