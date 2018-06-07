#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/contrib/hu/t0/RandFilter.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 14.12.2017
# Last Modified Date: 07.06.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>


from ampel.abstract.AbsAlertFilter import AbsAlertFilter
from random import randint

class RandFilter(AbsAlertFilter):


	version = 0.1


	def __init__(self, on_match_t2_units, base_config=None, run_config=None, logger=None):
		"""
		"""
		self.on_match_default_t2_units = on_match_t2_units

		if run_config is None:
			raise ValueError("run config required (threshold defined there)")

		self.threshold = run_config['threshold']

		if logger is not None:
			logger.info("RandFilter instantiated")


	def apply(self, ampel_alert):
		"""
		"""
		if randint(0, 99) > self.threshold:
			return self.on_match_default_t2_units
		else:
			return None
