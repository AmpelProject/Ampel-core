#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/common/AmpelUtils.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 07.06.2018
# Last Modified Date: 07.06.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>


class AmpelUtils():
	""" 
	"""

	@staticmethod
	def to_set(arg):
		"""
		converts input to set 
		"""
		return {el for el in arg} if type(arg) in (list, tuple) else {arg}
