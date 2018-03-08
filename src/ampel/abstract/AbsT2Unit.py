#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/abstract/AbsT2Unit.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 13.12.2017
# Last Modified Date: 08.03.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from ampel.abstract.AmpelABC import AmpelABC, abstractmethod

class AbsT2Unit(metaclass=AmpelABC):
	"""
	"""

	@abstractmethod
	def __init__(self, logger, base_config=None):
		pass

	@abstractmethod
	def run(self, light_curve, run_config=None):
		pass

	# pylint: disable=no-member
	def get_version(self):
		return self.version
