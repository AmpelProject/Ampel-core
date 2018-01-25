#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/contrib/hu/t0/NoFilter.py
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 14.12.2017
# Last Modified Date: 25.01.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>
from ampel.abstract.AbstractTransientFilter import AbstractTransientFilter

class NoFilter(AbstractTransientFilter):

	def set_filter_parameters(self, d):
		self.parameters = d

	def apply(self, ampel_alert):
		return True
