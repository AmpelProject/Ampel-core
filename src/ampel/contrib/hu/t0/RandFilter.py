#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/contrib/hu/t0/RandFilter.py
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 14.12.2017
# Last Modified Date: 27.01.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>
from ampel.abstract.AbstractTransientFilter import AbstractTransientFilter
from random import randint

class RandFilter(AbstractTransientFilter):

	version = 0.1

	def __init__(self):
		self.threshold = None

	def get_version(self):
		return RandFilter.version

	def set_filter_parameters(self, d):
		self.threshold = d['threshold']

	def apply(self, ampel_alert):
		if randint(0, 99) > self.threshold:
			return self.on_match_default_flags
		else:
			return None
