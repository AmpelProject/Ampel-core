#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/model/UnitModel.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 26.09.2019
# Last Modified Date: 10.06.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Dict, Optional, Any, Union, Type
from pydantic import validator
from ampel.base.AmpelBaseModel import AmpelBaseModel
from ampel.model.StrictModel import StrictModel
from ampel.base.DataUnit import DataUnit

class UnitModel(StrictModel):
	"""
	:param config:
	- None: no config
	- dict: config 'as is'
	- str: a corresponding alias key in the AmpelConfig must match the provided string
	- int: used internally for T2 units, a corresponding int key (AmpelConfig, base key 'confid') must match the provided integer

	:param override: allows the override of selected config keys
	"""

	unit: Union[str, Type[AmpelBaseModel]]
	config: Optional[Union[int, str, Dict[str, Any]]]
	override: Optional[Dict[str, Any]]

	# Optional UnitLoader to validate configs
	_unit_loader: Optional['UnitLoader'] = None

	@property
	def unit_name(self) -> str:
		if isinstance(self.unit, str):
			return self.unit
		return self.unit.__name__

	@validator('config')
	def validate_config(cls, v, values, **kwargs):
		if (unit := values.get('unit', None)) is None:
			return v
		elif isinstance(unit, str):
			if cls._unit_loader is None:
				return v
			unit = cls._unit_loader.get_class_by_name(values['unit'])
		if not isinstance(unit, DataUnit):
			return v
		config = dict()
		if v:
			config.update(v)
		if override := values.get('override', None):
			config.update(override)
		unit.validate(**config)
		return v
