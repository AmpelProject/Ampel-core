#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/t0/filters/RandFilter.py
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 14.12.2017
# Last Modified Date: 10.01.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>
from ampel.base.AbstractTransientFilter import AbstractTransientFilter
from ampel.flags.T2ModuleIds import T2ModuleIds
from random import randint

class RandFilter(AbstractTransientFilter):

	def __init__(self):
		self.threshold = None

	def set_filter_parameters(self, d):
		self.threshold = d['threshold']

	def apply(self, ampel_alert):
		if randint(0, 99) > self.threshold:
			return self.on_match_default_flags
		else:
			return None
