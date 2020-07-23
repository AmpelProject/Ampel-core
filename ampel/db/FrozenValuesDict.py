#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/db/FrozenValuesDict.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 18.12.2019
# Last Modified Date: 18.12.2019
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from ampel.view.ReadOnlyDict import ReadOnlyDict

def ro(arg):
	if arg.__class__ is list:
		return (*arg,)
	if arg.__class__ is FrozenValuesDict:
		return ReadOnlyDict(arg)
	return arg

class FrozenValuesDict(dict):
	"""
	Dict that recursively casts all values to immutable structures.
	Note that the dict itself is not Immutable
	"""

	def __setitem__(self, key, value):
		if isinstance(value, list):
			# cast to tuple
			super().__setitem__(key, (*[ro(el) for el in value],))
		elif isinstance(value, dict):
			super().__setitem__(key, ReadOnlyDict(value))
		else:
			super().__setitem__(key, value)
