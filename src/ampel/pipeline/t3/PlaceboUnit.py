#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel/src/ampel/pipeline/t3/PlaceboUnit.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 04.09.2018
# Last Modified Date: 04.09.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from ampel.base.abstract.AbsT3Unit import AbsT3Unit

class PlaceboUnit(AbsT3Unit):
	"""
	A T3 unit that does not perform any action.
	Goal: testing the T3 job/task machinery
	"""

	version = 1.0


	def __init__(self, logger, run_config=None, base_config=None, global_info=None):
		""" """
		self.logger = logger
		if global_info is not None:
			self.logger.info("Provided global info: %s" % global_info)


	def add(self, transients):
		""" """
		self.logger.info("Method add() called with %i transients" % len(transients))


	def done(self):
		""" """
		self.logger.info("Method done() called")
