#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/model/UnitModel.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 26.09.2019
# Last Modified Date: 01.08.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from contextlib import contextmanager
from typing import Dict, Optional, Any, Union, Type, Sequence, cast, TYPE_CHECKING
from pydantic import create_model, root_validator
from ampel.base.AmpelBaseModel import AmpelBaseModel
from ampel.model.StrictModel import StrictModel

if TYPE_CHECKING:
	from ampel.core.UnitLoader import UnitLoader


class UnitModel(StrictModel):
	"""
	Specification of a processing unit to instantiate.
	"""

	#: Name of registered unit class, or class itself
	unit: Union[str, Type[AmpelBaseModel]]
	#: - None: no config (use class defaults)
	#: - dict: config 'as is'
	#: - str: a corresponding alias key in the AmpelConfig must match the provided string
	#: - int: used internally for T2 units, a corresponding int key (AmpelConfig, base key 'confid') must match the provided integer
	config: Optional[Union[int, str, Dict[str, Any]]]
	#: Values to override in the config
	override: Optional[Dict[str, Any]]

	# Optional static UnitLoader to validate configs
	_unit_loader: Optional['UnitLoader'] = None


	@property
	def unit_name(self) -> str:
		if isinstance(self.unit, str):
			return self.unit
		return self.unit.__name__

	@classmethod
	@contextmanager
	def validate_configs(cls, unit_loader: "Optional[UnitLoader]"):
		"""Temporarily enable unit model validataion"""
		prev = cls._unit_loader
		cls._unit_loader = unit_loader
		yield
		cls._unit_loader = prev

	@root_validator
	def validate_config(cls, values: Dict[str,Any]) -> Dict[str,Any]:
		if cls._unit_loader:
			from ampel.base.DataUnit import DataUnit
			from ampel.core.AdminUnit import AdminUnit
			from ampel.abstract.AbsProcessorUnit import AbsProcessorUnit
			from ampel.abstract.ingest.AbsIngester import AbsIngester
			from ampel.t3.run.AbsT3UnitRunner import AbsT3UnitRunner
			from ampel.t3.context.AbsT3RunContextAppender import AbsT3RunContextAppender

			unit = cls._unit_loader.get_class_by_name(values['unit'])
			if issubclass(unit, AbsIngester):
				# AbsIngester requires runtime parameters not in the config
				...
			elif issubclass(unit, (DataUnit, AdminUnit, AbsProcessorUnit)):
				# exclude base class fields provided at runtime
				exclude = {"logger"}
				for parent in cast(
					Sequence[Type[AmpelBaseModel]],
					(
						DataUnit,
						AdminUnit,
						AbsT3UnitRunner,
						AbsT3RunContextAppender,
					)
				):
					if issubclass(unit, parent):
						exclude.update(parent._annots.keys())
				fields = {
					k: (v, unit._defaults[k] if k in unit._defaults else ...)
					for k, v in unit._annots.items() if k not in exclude
				} # type: ignore
				model = create_model(
					unit.__name__, __config__ = StrictModel.__config__,
					__base__=None, __module__=None, __validators__=None,
					**fields
				)
				model.validate(
					cls._unit_loader.get_init_config(
						values['unit'],
						values['config'],
						values['override']
					)
				)
		return values
