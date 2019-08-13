#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel/src/ampel/pipeline/t1/T1Controller.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 06.12.2018
# Last Modified Date: 06.12.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import logging
from ampel.core.flags.LogRecordFlags import LogRecordFlags
from ampel.pipeline.logging.AmpelLogger import AmpelLogger
from ampel.pipeline.logging.DBLoggingHandler import DBLoggingHandler
from ampel.pipeline.db.AmpelDB import AmpelDB
from ampel.pipeline.common.Schedulable import Schedulable


class T1Controller(Schedulable):
	"""
	"""

	def __init__(self, channels=None, check_interval=120, log_level=logging.DEBUG): 
		"""
		:param int check_interval: in minutes
		"""

		# Get logger 
		self.logger = AmpelLogger.get_unique_logger(log_level=log_level)

		# check interval in seconds
		self.check_interval = check_interval

		# Parent constructor
		Schedulable.__init__(self)

		# Schedule processing of t2 docs
		self.get_scheduler().every(check_interval).minutes.do(
			self.auto_complete
		)

		# Shortcut
		self.col_beacon = AmpelDB.get_collection('beacon')
		self.col_tran = AmpelDB.get_collection('tran')

		# Create t1Controller beacon doc if it does not exist yet
		self.col_beacon.update_one(
			{'_id': "t1Controller"},
			{'$set': {'_id': "t1Controller"}},
			upsert=True
		)


	def auto_complete(self):
		"""
		"""

		# Create DB logging handler instance (logging.Handler child class)
		# This class formats, saves and pushes log records into the DB
		db_logging_handler = DBLoggingHandler(
			LogRecordFlags.T1 | LogRecordFlags.CORE | LogRecordFlags.SCHEDULED_RUN
		)

		# Add db logging handler to the logger stack of handlers 
		self.logger.addHandler(db_logging_handler)

		# Soon...
