#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/abstract/AbsCompoundShaper.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 07.05.2018
# Last Modified Date: 13.06.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from ampel.abstract.AmpelABC import AmpelABC, abstractmethod

class AbsCompoundShaper(metaclass=AmpelABC):
	"""
	"""

	@abstractmethod
	def gen_compound_item(self, input_d, channel_name):
		""" """
		pass
