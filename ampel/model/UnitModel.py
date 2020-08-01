#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/model/UnitModel.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 26.09.2019
# Last Modified Date: 01.08.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Dict, Optional, Any, Union, Type, TYPE_CHECKING
from pydantic import root_validator
from ampel.base.AmpelBaseModel import AmpelBaseModel
from ampel.model.StrictModel import StrictModel
from ampel.base.DataUnit import DataUnit
if TYPE_CHECKING:
	from ampel.core.UnitLoader import UnitLoader


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

	# Optional static UnitLoader to validate configs
	_unit_loader: Optional['UnitLoader'] = None


	@property
	def unit_name(self) -> str:
		if isinstance(self.unit, str):
			return self.unit
		return self.unit.__name__


	@root_validator
	def validate_config(cls, values):
		if cls._unit_loader:
			unit = cls._unit_loader.get_class_by_name(values['unit'])
			if issubclass(unit, DataUnit):
				if not unit._model:
					unit._create_model()
				unit._model.validate(cls._unit_loader.get_init_config(values['config'], values['override']))
		return values
