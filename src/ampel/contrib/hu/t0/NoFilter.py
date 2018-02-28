#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/contrib/hu/t0/NoFilter.py
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 14.12.2017
# Last Modified Date: 28.02.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>
from ampel.abstract.AbstractTransientFilter import AbstractTransientFilter

class NoFilter(AbstractTransientFilter):

	def set_filter_parameters(self, d):
		pass

	def get_version(self):
		return 1.0

	def apply(self, ampel_alert):
		return self.on_match_default_flags
