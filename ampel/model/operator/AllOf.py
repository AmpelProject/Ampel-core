#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/model/operator/AllOf.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 15.10.2018
# Last Modified Date: 13.02.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from pydantic.generics import GenericModel
from typing import List, Generic
from ampel.type import T


class AllOf(GenericModel, Generic[T]):
	#: Select items by logical AND
	all_of: List[T]
