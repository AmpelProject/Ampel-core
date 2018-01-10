#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : /Users/hu/Documents/ZTF/Ampel/src/ampel/pipeline/common/LoggingUtils.py
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 14.12.2017
# Last Modified Date: 14.12.2017
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>
import logging
from ampel.pipeline.common.db.DBLoggingHandler import DBLoggingHandler


class LoggingUtils:
	"""
		Class containing static util methods related to the pipeline logging mechanisms
	"""

	@staticmethod
	def get_ampel_console_logger():
		"""
			Returns a logger (registered as 'Ampel' in the module logging)
			with the following parameters:
				* Log level DEBUG
				* Logging format:
					'%(asctime)s %(filename)s:%(lineno)s %(funcName)s() %(levelname)s %(message)s'
		"""
		if "Ampel" in logging.Logger.manager.loggerDict:
			logging.Logger.manager.loggerDict.pop("Ampel")

		logger = logging.getLogger("Ampel")
		shandler = logging.StreamHandler()
		shandler.setLevel(logging.DEBUG)
		formatter = logging.Formatter(
			'%(asctime)s %(filename)s:%(lineno)s %(funcName)s() %(levelname)s %(message)s',
			"%Y-%m-%d %H:%M:%S"
		)
		shandler.setFormatter(formatter)

		logger.addHandler(shandler)
		logger.setLevel(logging.DEBUG)

		return logger

	@staticmethod
	def add_db_log_handler(logger, db_job_info):
		"""
			Adds a new logging handler (instance of common.db.DBLoggingHandler) to the logger 'Ampel'.
			Argument must be an instance of common.db.DBJobInfo
		"""
		logger.info("Attaching mongo log handler to Ampel logger")
		dblh = DBLoggingHandler(db_job_info)
		dblh.setLevel(logging.DEBUG)
		dblh.setFormatter(logging.Formatter('%(message)s'))
		logger.addHandler(dblh)

		return dblh


	@staticmethod
	def cosmetic_flags(arg):
		"""
			Cosmetic method used to convert default output

				In []: type(alert)
				Out[]: t0.TransientCandidate.TransientCandidate

				In []: alert.flags
				Out[]: <TransientFlags.AUTO_COMPLETE|FILTER_ACCEPTED|FILTER_RANDOM|ALERT_ZTF: 12417>

			into

				In []: LoggingUtils.cosmetic_flags(alert.flags)
				Out[]: 'ALERT_ZTF | FILTER_RANDOM | FILTER_ACCEPTED | AUTO_COMPLETE'

		"""
		mystr = ""
		flags = str(arg).replace("TransientFlags.", "").split("|")
		for flag in reversed(flags):
			mystr += flag + " | "
		return mystr[:-3]

