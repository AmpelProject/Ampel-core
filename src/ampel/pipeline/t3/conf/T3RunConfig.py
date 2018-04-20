#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/t3/conf/T3RunConfig.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 06.03.2018
# Last Modified Date: 11.03.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from ampel.pipeline.logging.LoggingUtils import LoggingUtils

class T3RunConfig:
	"""
	"""

	# Static dict holding references to previously instanciated t3RunConfig
	# key = unit_id, value = t3RunConfig instance
	_loaded_run_conf = {}


	@classmethod # Class method creating or returning previously instanciated t3RunConfigs
	def load(cls, run_conf_id, mongo_col, logger=None):
		"""
		"""
		if run_conf_id in cls._loaded_run_conf:
			return cls._loaded_run_conf[run_conf_id]
		else:
			cls._loaded_run_conf[run_conf_id] = T3RunConfig(mongo_col, run_conf_id, logger)
			return cls._loaded_run_conf[run_conf_id] 		
		

	def __init__(self, mongo_col, run_conf_id, logger=None):
		"""
		"""
		
		if logger is None:
			logger = LoggingUtils.get_logger()

		self.id = run_conf_id

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
		logger.info(
			"Loaded T3 run config '%s': '%s'" % 
			(run_conf_id, self.doc)
		)


	def get_parameters(self):
		return self.doc


	def get_parameter(self, name):
		""" 
		"""
		return self.doc[name] if name in self.doc else None
