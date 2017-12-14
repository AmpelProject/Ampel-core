#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/t0/filters/LensFilter.py
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 14.12.2017
# Last Modified Date: 14.12.2017
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>
from ampel.pipeline.t0.filters.AbstractTransientsFilter import AbstractTransientsFilter
from random import randint

class RandFilter(AbstractTransientsFilter):

	def __init__(self):
		self.threshold = None

	def set_filter_parameters(self, d):
		self.threshold = d['threshold']

	def apply(self, ztfdict):
		if randint(0, 99) > self.threshold:
			return self.on_match_default_flags
		else:
			return None
