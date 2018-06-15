#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/base/Frozen.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 01.03.2018
# Last Modified Date: 07.06.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

class Frozen:

	def __setattr__(self, key, value):
		"""
		Overrride python's default __setattr__ method to enable frozen instances
		"""
		# '_%s__isfrozen' and not simply '__isfrozen' because: 'Private name mangling'
		if getattr(self, "_%s__isfrozen" % self.__class__.__name__, None) is not None:
			raise TypeError( "%r is a frozen instance " % self )
		object.__setattr__(self, key, value)
