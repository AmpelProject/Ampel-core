#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/contrib/hu/t0/NeutrinoFilter.py
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 14.12.2017
# Last Modified Date: 25.01.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>
from ampel.abstract.AbstractTransientFilter import AbstractTransientFilter

class NeutrinoFilter(AbstractTransientFilter):

	def __init__(self):
		return

	def set_cut_values(self, arg):
		return

	def apply(self, ampel_alert):
		return True
