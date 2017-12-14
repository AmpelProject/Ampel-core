#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : /Users/hu/Documents/ZTF/Ampel/src/ampel/pipeline/t0/filters/SNFilter.py
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 14.12.2017
# Last Modified Date: 14.12.2017
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>
from ampel.pipeline.t0.filters.AbstractTransientsFilter import AbstractTransientsFilter

class SNFilter(AbstractTransientsFilter):

	def set_filter_parameters(self, d):
		self.parameters = d

	def apply(self, ampel_alert):
		for el in ampel_alert.get_photopoints():
			if el['magpsf'] < 18:
				return self.on_match_default_flags
		return None
