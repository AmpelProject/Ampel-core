#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/model/operator/AnyOf.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 15.10.2018
# Last Modified Date: 15.02.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import collections
from typing import Union, List, Generic
from pydantic import validator
from pydantic.generics import GenericModel
from ampel.type import T
from ampel.model.operator.AllOf import AllOf


class AnyOf(GenericModel, Generic[T]):

	#: Select items by logical OR
	any_of: List[Union[T, AllOf[T]]]

	@validator('any_of', pre=True)
	def cast_to_list(cls, v):
		if not isinstance(v, collections.abc.Sequence):
			return [v]
		return v
