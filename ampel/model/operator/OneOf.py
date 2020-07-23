#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/model/operator/OneOf.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 22.10.2018
# Last Modified Date: 13.02.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import List, Generic
from pydantic.generics import GenericModel
from ampel.type import T


class OneOf(GenericModel, Generic[T]):
	one_of: List[T]
