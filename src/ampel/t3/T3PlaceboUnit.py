#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/t3/T3PlaceboUnit.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 04.09.2018
# Last Modified Date: 15.09.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from ampel.base.abstract.AbsT3Unit import AbsT3Unit

class T3PlaceboUnitError(RuntimeError):
	pass

class T3PlaceboUnit(AbsT3Unit):
	"""
	A T3 unit that does not perform any action.
	Goal: testing the T3 job/task machinery
	"""

	def __init__(self, logger, base_config=None, run_config=None, global_info=None):
		""" """
		self.logger = logger
		if global_info is not None:
			self.logger.info("Provided global info: %s" % global_info)
		self._raise = (run_config if run_config else {}).get('raise', False)

	def add(self, transients):
		""" """
		self.logger.info("Method add() called with %i transient(s)" % len(transients))


	def done(self):
		""" """
		self.logger.info("Method done() called")
		if self._raise:
			raise T3PlaceboUnitError
