#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/contrib/hu/t0/SNFilter.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 14.12.2017
# Last Modified Date: 08.03.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from ampel.abstract.AbsAlertFilter import AbsAlertFilter

class SNFilter(AbsAlertFilter):
	"""
	ALpha non-sense filter for the moment
	"""

	version = 1.0

	def __init__(self, on_match_t2_units, base_config=None, run_config=None, logger=None):
		"""
		"""
		self.on_match_default_t2_units = on_match_t2_units
		self.base_config = base_config
		self.run_config = run_config


	def apply(self, ampel_alert):

		for el in ampel_alert.get_photopoints():
			if el['magpsf'] < 18:
				return self.on_match_default_t2_units

		return None
