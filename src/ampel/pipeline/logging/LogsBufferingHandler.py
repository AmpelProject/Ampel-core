#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/logging/LogsBufferingHandler.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 27.09.2018
# Last Modified Date: 18.01.2019
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import logging, struct, os
from bson import ObjectId
from ampel.pipeline.db.AmpelDB import AmpelDB
from ampel.pipeline.logging.AmpelLoggingError import AmpelLoggingError

class LogsBufferingHandler(logging.Handler):
	"""
	"""

	def __init__(self, tier, level=logging.DEBUG, aggregate_interval=1):
		""" 
		:param int aggregate_interval: logs with equals attributes 
		(log level, possibly tranId & alertId) are put together in one document instead
		of being splitted in one document per log entry (spares some index RAM).
		aggregate_interval is the max interval in seconds during which the log aggregation
		takes place. Beyond this value, a new log document is created no matter what.
		Default value is 1.
		"""

		self.filters = []  # required when extending logging.Handler
		self.lock = None   # required when extending logging.Handler
		self._name = None
		self.setLevel(level)

		self.aggregate_interval = aggregate_interval
		self.tier = tier
		self.log_dicts = []
		self.last_log_dict = None

		# ObjectID middle: 3 bytes machine + # 2 random bytes 
		# NB: pid is not always unique if running in a jail or container
		self.oid_middle = ObjectId._machine_bytes + os.urandom(2)


	def emit(self, record):
		""" 
		:raises AmpelLoggingError:
		"""

		try: 
			# Same flag, date (+- 1 sec), tran_id and chans
			if (
				self.last_log_dict and 
				record.levelno == self.last_log_dict['lvl'] and 
				record.created - struct.unpack(">i", self.last_log_dict['_id'].binary[0:4])[0] < 
				self.aggregate_interval
			):
	
				rec = self.last_log_dict
				if type(rec['msg']) is not list:
					rec['msg'] = [rec['msg']]
	
				rec['msg'].append(record.msg)
	
			else:
	
				# Generate object id with log record.created as current time
				with ObjectId._inc_lock:
					oid = struct.pack(">i", int(record.created)) + \
						  self.oid_middle + \
						  struct.pack(">i", ObjectId._inc)[1:4]
					ObjectId._inc = (ObjectId._inc + 1) % 0xFFFFFF
	
				rec = {
					'_id': ObjectId(oid=oid),
					'tier': self.tier,
					'lvl': record.levelno,
					'msg': record.msg
				}
	
				if record.levelno > logging.INFO:
					rec['filename'] = record.filename,
					rec['lineno'] = record.lineno,
					rec['funcName'] = record.funcName,
			
				self.log_dicts.append(rec)
				self.last_log_dict = rec

		except Exception as e:
			
			from ampel.pipeline.logging.AmpelLogger import AmpelLogger
			from ampel.pipeline.logging.LoggingUtils import LoggingUtils

			# Print log stack using std logging 
			logger = AmpelLogger.get_unique_logger()
			LoggingUtils.log_exception(logger, e, msg="Primary exception:")

			raise AmpelLoggingError from None
