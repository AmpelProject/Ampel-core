#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/model/AliasedUnitModel.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 19.02.2020
# Last Modified Date: 02.03.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Union, Dict, Any, Optional
from pydantic import StrictInt, StrictStr
from ampel.model.PlainUnitModel import PlainUnitModel


class AliasedUnitModel(PlainUnitModel):
	"""
	* Redefines the field 'config' of superclass PlainUnitModel
	into something that is not a dict but an alias:
	- str: a corresponding alias key must match the provided string
	- int: should not be used (used internally for T2 units, a corresponding
	t2 config key must match the provided integer)

	*Adds definition of 'override' which allows the override of selected config kyes

	Note: From the superclass field definitions remains 'unit' untouched
	"""
	# we override parent's type
	config: Union[StrictInt, StrictStr] # type: ignore
	override: Optional[Dict[str, Any]]
