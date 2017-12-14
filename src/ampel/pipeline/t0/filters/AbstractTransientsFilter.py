#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : /Users/hu/Documents/ZTF/Ampel/src/ampel/pipeline/t0/filters/AbstractTransientsFilter.py
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 14.12.2017
# Last Modified Date: 14.12.2017
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>
from abc import ABC, abstractmethod
from ampel.pipeline.common.flags.TransientFlags import TransientFlags

class AbstractTransientsFilter(ABC):

	on_match_default_flags = TransientFlags(0)

	def set_log_record_flag(self, flag):
		self.log_record_flag = flag

	def set_on_match_default_flags(self, flags):
		self.on_match_default_flags = flags

	@abstractmethod
	def set_filter_parameters(self, dict_instance):
		pass

	@abstractmethod
	def apply(self, transient_candidate):
		pass
