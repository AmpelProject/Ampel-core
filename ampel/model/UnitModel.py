#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/model/UnitModel.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 26.09.2019
# Last Modified Date: 10.06.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Dict, Optional, Any, Union, Type
from ampel.base.AmpelUnit import AmpelUnit
from ampel.model.AmpelStrictModel import AmpelStrictModel


class UnitModel(AmpelStrictModel):
	"""
	:param config:
	- None: no config
	- dict: config 'as is'
	- str: a corresponding alias key in the AmpelConfig must match the provided string
	- int: used internally for T2 units, a corresponding int key (AmpelConfig, base key 'confid') must match the provided integer

	:param override: allows the override of selected config keys
	"""

	unit: Union[str, Type[AmpelUnit]]
	config: Optional[Union[int, str, Dict[str, Any]]]
	override: Optional[Dict[str, Any]]

	@property
	def unit_name(self) -> str:
		if isinstance(self.unit, str):
			return self.unit
		return self.unit.__name__
