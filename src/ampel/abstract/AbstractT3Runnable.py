#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/abstract/AbstractT3Runnable.py
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 23.02.2018
# Last Modified Date: 23.02.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from ampel.abstract.AmpelABC import AmpelABC, abstractmethod

class AbstractT3Runnable(metaclass=AmpelABC):
	"""
	"""

	def set_log_record_flag(self, flag):
		self.log_record_flag = flag

	def set_logger(self, logger):
		self.logger = logger

	@abstractmethod
	def run(self, transient_collection, run_parameters):
		pass

	@abstractmethod
	def get_version(self):
		pass
