#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/t0/alerts/JSONDeserializer.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 30.04.2018
# Last Modified Date: 30.04.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from ampel.abstract.AbsAlertDeserializer import AbsAlertDeserializer
import json

class JSONDeserializer(AbsAlertDeserializer):
	"""
	"""

	def get_dict(self, bytes_in):
		"""	
		Returns a dict. See AbsAlertParser docstring for more info
		"""
		return json.load(bytes_in)
