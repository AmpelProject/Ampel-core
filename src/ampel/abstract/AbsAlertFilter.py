#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/abstract/AbsAlertFilter.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 14.12.2017
# Last Modified Date: 08.03.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from ampel.abstract.AmpelABC import AmpelABC, abstractmethod
import logging


class AbsAlertFilter(metaclass=AmpelABC):


	@abstractmethod
	def __init__(self, on_match_t2_units, base_config=None, run_config=None, logger=None):
		pass

	@abstractmethod
	def apply(self, ampel_alert):
		pass

	# pylint: disable=no-member
	def get_version(self):
		return self.version
