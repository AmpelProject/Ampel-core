#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/t3/conf/T3TaskConfig.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 06.03.2018
# Last Modified Date: 08.03.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from ampel.pipeline.logging.LoggingUtils import LoggingUtils
from ampel.pipeline.t3.conf.T3UnitConfig import T3UnitConfig
from ampel.pipeline.t3.conf.T3RunConfig import T3RunConfig

class T3TaskConfig:
	"""
	"""

	# static DB collection names
	run_config_db_name = "t3_run_config"
	t3_units_db_name = "t3_units"


	def __init__(self, config_db, t3_task_doc, logger=None):
		"""
		"""

		self.logger = LoggingUtils.get_logger() if logger is None else logger

		# Creating T3UnitConfig instance
		self.t3_unit = T3UnitConfig.load(
			config_db[T3TaskConfig.t3_units_db_name], 
			t3_task_doc['t3Unit'], 
			logger
		)

		# Creating T3RunConfig instance
		self.run_config = T3RunConfig.load(
			config_db[T3TaskConfig.run_config_db_name], 
			t3_task_doc['runConfig'], 
			logger
		)

		# Save transient sub-selection criteria if provided
		if "select" in t3_task_doc:
			self.select = t3_task_doc['select']

		# Feedback
		self.logger.info(
			"Loaded t3 task associated with t3 unit '%s' and runConfig '%s'" %
			(t3_task_doc['t3Unit'], t3_task_doc['runConfig'])
		)
			
	def get_selection(self):
		"""
		Returns transient sub-selection criteria, if available
		"""
		return self.select if hasattr(self, "select") else None
