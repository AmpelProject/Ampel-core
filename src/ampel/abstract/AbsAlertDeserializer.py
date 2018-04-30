#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/abstract/AbsAlertDeserializer.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 24.04.2018
# Last Modified Date: 30.04.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from abc import ABC, abstractmethod

class AbsAlertDeserializer(ABC):


	@abstractmethod
	def get_dict(self, in_bytes):
		"""
		"""
		pass
