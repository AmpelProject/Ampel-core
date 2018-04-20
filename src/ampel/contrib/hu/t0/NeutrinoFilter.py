#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/contrib/hu/t0/NeutrinoFilter.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 14.12.2017
# Last Modified Date: 08.03.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from ampel.abstract.AbsAlertFilter import AbsAlertFilter

class NeutrinoFilter(AbsAlertFilter):

	version = 0.0

	def __init__(self, on_match_t2_units, base_config=None, run_config=None, logger=None):
		self.on_match_default_t2_units = on_match_t2_units

	def apply(self, ampel_alert):
		# TODO: implement
		return None
