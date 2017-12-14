#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : /Users/hu/Documents/ZTF/Ampel/src/ampel/pipeline/t0/filters/NoFilter.py
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 14.12.2017
# Last Modified Date: 14.12.2017
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>
from ampel.pipeline.t0.filters.AbstractTransientsFilter import AbstractTransientsFilter

class NoFilter(AbstractTransientsFilter):

	def __init__(self):
		return

	def set_cut_values(self, arg):
		return

	def passes(self, transient_candidate):
		return True
