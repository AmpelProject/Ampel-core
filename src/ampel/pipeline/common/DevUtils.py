#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/common/DevUtils.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 11.07.2018
# Last Modified Date: 11.07.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from types import MappingProxyType

class DevUtils():
	""" 
	"""

	@classmethod
	def recursive_unfreeze(cls, arg):
		"""
		Inverse of AmpelUtils.recursice_freeze
		"""
		if isinstance(arg, MappingProxyType):
			return dict(
				{
					cls.recursive_unfreeze(k): cls.recursive_unfreeze(v) 
					for k,v in arg.items()
				}
			)

		elif isinstance(arg, tuple):
			return list(
				map(cls.recursive_unfreeze, arg)
			)

		elif isinstance(arg, frozenset):
			return set(arg)

		else:
			return arg
