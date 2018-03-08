#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/t3/conf/T3RunConfig.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 06.03.2018
# Last Modified Date: 08.03.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from ampel.pipeline.logging.LoggingUtils import LoggingUtils

class T3RunConfig:
	"""
	"""

	# Static dict holding references to previously instanciated t3RunConfig
	# key = unit_id, value = t3RunConfig instance
	loaded_run_conf = {}


	@staticmethod # Static method creating or returning previously instanciated t3RunConfigs
	def load(cls, mongo_col, run_conf_id, logger=None):
		"""
		"""
		if run_conf_id in cls.loaded_run_conf:
			return cls.loaded_run_conf[run_conf_id]
		else:
			cls.loaded_run_conf[run_conf_id] = T3RunConfig(mongo_col, run_conf_id, logger)
			return cls.loaded_run_conf[run_conf_id] 		
		

	def __init__(self, mongo_col, run_conf_id, logger=None):
		"""
		"""
		
		self.logger = LoggingUtils.get_logger() if logger is None else logger
		self.run_conf_id = run_conf_id

		# Lookup t3 run config document in DB
		cursor = mongo_col.find(
			{'_id': run_conf_id}
		)

		# Robustness check
		if cursor.count() == 0:
			raise ValueError(
				"T3 run config '%s' not found" % run_conf_id
			)

		# Retrieve t3 run config document
		self.doc = next(cursor)

		# Feedback
		self.logger.info(
			"T3 run config '%s' loaded: '%s'" % 
			(run_conf_id, self.doc)
		)


	def get_parameter(self, name):
		""" 
		"""
		return self.doc[name] if name in self.doc else None
