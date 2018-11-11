#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/logging/AmpelLoggingStreamHandler.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 17.10.2018
# Last Modified Date: 11.11.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from logging import StreamHandler, DEBUG, _logRecordFactory
from ampel.pipeline.logging.AmpelLogger import AmpelLogger
	
class AmpelLoggingStreamHandler(StreamHandler):


	def __init__(self, stream, level=DEBUG, aggregate_interval=1):
		""" 
		:param int aggregate_interval: logs with similar attributes (log level, ...) 
		are aggregated in one document instead of being split
		into several documents (spares some index RAM). *aggregate_interval* is the max interval 
		of time in seconds during which log aggregation takes place. Beyond this value, 
		a new log document is created no matter what. It thus impacts the logging time granularity.
		"""

		super().__init__(stream)
		self.aggregate_interval = aggregate_interval
		self.prev_records = _logRecordFactory(None, None, None, None, None, None, None, None, None)
		self.setLevel(level)


	def emit(self, record):
		""" 
		"""

		extra = getattr(record, 'extra', None)

		# Same flag, date (+- 1 sec), tran_id and chans
		if (
			AmpelLogger.aggregation_ok and 
			self.prev_records and 
			record.levelno == self.prev_records.levelno and 
			record.filename == self.prev_records.filename and 
			record.created - self.prev_records.created < self.aggregate_interval and 
			extra == getattr(self.prev_records, 'extra', None) and
			record.msg
		):
			self.stream.write(str(record.msg))
		else:

			self.stream.write(self.format(record))
			self.prev_records = record

		#self.stream.write(self.terminator)
		self.flush()
