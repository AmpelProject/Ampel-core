#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/model/operator/AllOf.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 15.10.2018
# Last Modified Date: 13.02.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from pydantic.generics import GenericModel
from typing import List, TypeVar, Generic
from pydantic import StrictInt, StrictStr, StrictFloat

T = TypeVar("T", StrictInt, StrictStr, StrictFloat, bytes)

class AllOf(GenericModel, Generic[T]):
	all_of: List[T]
