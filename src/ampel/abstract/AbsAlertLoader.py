#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/abstract/AbsAlertLoader.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 24.04.2018
# Last Modified Date: 24.04.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from abc import ABC, abstractmethod

class AbsAlertLoader(ABC):


	@abstractmethod
	def load_alerts(self):
		pass
