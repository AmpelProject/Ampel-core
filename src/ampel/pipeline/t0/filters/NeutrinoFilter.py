#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/t0/filters/NeutrinoFilter.py
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 14.12.2017
# Last Modified Date: 08.01.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>
from ampel.pipeline.t0.filters.AbstractTransientsFilter import AbstractTransientsFilter

class NeutrinoFilter(AbstractTransientsFilter):

	def __init__(self):
		return

	def set_cut_values(self, arg):
		return

	def apply(self, ampel_alert):
		return True
