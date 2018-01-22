#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/base/AbstractTransientFilter.py
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 14.12.2017
# Last Modified Date: 21.01.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>
from ampel.base.AmpelABC import AmpelABC, abstractmethod
from ampel.flags.T2ModuleIds import T2ModuleIds
import logging

logger = logging.getLogger("Ampel")

class AbstractTransientFilter(metaclass=AmpelABC):

	def set_log_record_flag(self, flag):
		self.log_record_flag = flag

	def set_on_match_default_flags(self, flags):
		self.on_match_default_flags = flags

	def set_logger(self, logger):
		self.logger = logger

	@abstractmethod
	def get_version(self):
		pass

	@abstractmethod
	def set_filter_parameters(self, dict_instance):
		pass

	@abstractmethod
	def apply(self, ampel_alert):
		pass
