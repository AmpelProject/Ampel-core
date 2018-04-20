#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/t3/T3QueryPlanner.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 26.02.2018
# Last Modified Date: 11.03.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from ampel.pipeline.logging.LoggingUtils import LoggingUtils

class T3QueryPlanner:
	"""
	"""

	def __init__(self, db, t3_job, logger=None, collection="main"):

		self.col = db[collection]
		self.logger = LoggingUtils.get_logger() if logger is None else logger

		load_options = t3_job.tran_load_options()

		if load_options["state"] == "latest":
			# retrive latest state for transients	
			pass
		# all states
		else:
			pass
