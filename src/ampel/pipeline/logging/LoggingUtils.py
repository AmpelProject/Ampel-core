#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/logging/LoggingUtils.py
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 14.12.2017
# Last Modified Date: 23.01.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>
import logging, sys
from datetime import datetime


class LoggingUtils:
	"""
		Logging related static util method(s)
	"""

	@staticmethod
	def get_logger(unique=False):
		"""
			Returns a logger (registered as 'Ampel' in the module logging is unique=False)
			with the following parameters:
				* Log level DEBUG
				* Logging format:
					'%(asctime)s %(filename)s:%(lineno)s %(funcName)s() %(levelname)s %(message)s'
		"""

		logging.basicConfig(
			format='%(asctime)s %(filename)s:%(lineno)s %(funcName)s() %(levelname)s %(message)s', 
			datefmt="%Y-%m-%d %H:%M:%S", 
			level=logging.DEBUG,
			stream=sys.stdout
		)

		logger = logging.getLogger(
			"Ampel-"+str(datetime.now().time()) if unique is True else "Ampel"
		)

		return logger
