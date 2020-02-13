#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/model/operator/AnyOf.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 15.10.2018
# Last Modified Date: 13.02.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Union, List, Generic, TypeVar
from pydantic.generics import GenericModel
from pydantic import StrictInt, StrictStr, StrictFloat
from ampel.model.operator.AllOf import AllOf

T = TypeVar("T", StrictInt, StrictStr, StrictFloat, bytes)

class AnyOf(GenericModel, Generic[T]):
	any_of: List[Union[T, AllOf]]
