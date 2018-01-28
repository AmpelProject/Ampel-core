#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/abstract/AbstractT2Runnable.py
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 13.12.2017
# Last Modified Date: 28.01.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>
from ampel.abstract.AmpelABC import AmpelABC, abstractmethod

class AbstractT2Runnable(metaclass=AmpelABC):
	"""
	"""

	def set_log_record_flag(self, flag):
		self.log_record_flag = flag

	def set_logger(self, logger):
		self.logger = logger

	@abstractmethod
	def run(self, light_curve, run_parameters):
		pass

	@abstractmethod
	def get_version(self):
		pass
