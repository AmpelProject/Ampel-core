#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/utils/general.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 10.04.2020
# Last Modified Date: 10.04.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Dict, Type, Literal, Union
from ampel.type import strict_iterable, StrictIterable


def has_nested_type(obj: StrictIterable, target_type: Type, strict: bool = True) -> bool:
	"""
	:param obj: object instance (dict/list/set/tuple)
	:param target_type: example: ReadOnlyDict/list
	:param strict: must be an instance of the provided type (subclass instances would be rejected)
	"""

	if strict:
		# pylint: disable=unidiomatic-typecheck
		if type(obj) is target_type:
			return True
	else:
		if isinstance(obj, target_type):
			return True

	if isinstance(obj, dict):
		for el in obj.values():
			if has_nested_type(el, target_type):
				return True

	elif isinstance(obj, strict_iterable):
		for el in obj:
			if has_nested_type(el, target_type):
				return True

	return False
