#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/util/template.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 16.07.2021
# Last Modified Date: 16.07.2021
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from importlib import import_module
from functools import cache
from typing import List, Dict, Any, Type, Union, Sequence, Optional
from ampel.abstract.AbsTiedT2Unit import AbsTiedT2Unit
from ampel.model.UnitModel import UnitModel
from ampel.model.ingest.T2Compute import T2Compute
from ampel.config.builder.FirstPassConfig import FirstPassConfig


def check_tied_units(
		all_t2_units: List[T2Compute],
		first_pass_config: Union[FirstPassConfig, dict[str,Any]]
	) -> None:
		"""
		:raises: ValueError if tied t2 units are present in t2_units but the requred t2 units are not present in t2_compute
		"""
		tied_units: List[T2Compute] = []
		for el in all_t2_units:
			if "AbsTiedT2Unit" in first_pass_config['unit'][el.unit]['base']:
				tied_units.append(el)
		
		def as_unitmodel(t2_unit_model: T2Compute) -> UnitModel:
			return UnitModel(**{k: v for k,v in t2_unit_model.dict().items() if k in UnitModel.__fields__})
		
		@cache
		def get_default_dependencies(unit: str) -> list[dict[str,Any]]:
			klass: Type[AbsTiedT2Unit] = getattr(
				import_module(
					first_pass_config['unit'][unit]['fqn']
				),
				unit
			)
			return [dep.dict() for dep in klass.t2_dependency]

		for tied_unit in tied_units:
			t2_deps = (
				(tied_unit.config if isinstance(tied_unit.config, dict) else {}) | \
				(tied_unit.override or {})
			).get("t2_dependency") or get_default_dependencies(tied_unit.unit)
			for t2_dep in t2_deps:
				dependency_config = UnitModel(unit=t2_dep["unit"], config=t2_dep.get("config"), override=t2_dep.get("override"))
				candidates = [as_unitmodel(unit) for unit in all_t2_units if unit.unit == dependency_config.unit]
				if not any((c == dependency_config for c in candidates)):
					raise ValueError(
						f"Unit {tied_unit.unit} depends on unit {dependency_config.dict()}, "
						f"which was not configured. Possible matches are: "
						f"{[c.dict() for c in candidates]}"
					)

def filter_units(
	units: Sequence[UnitModel],
	abs_unit: Union[str, List[str]],
	config: Union[FirstPassConfig, Dict[str, Any]]
) -> List[Dict]:
	"""
	:returns: unit defintions (dict) that are subclass of the provided abstract class name.
	"""

	if isinstance(abs_unit, str):
		abs_unit = [abs_unit]

	return [
		el.dict(exclude_unset=True, by_alias=True)
		for el in units
		if any(
			unit in config['unit'][el.unit]['base']
			for unit in abs_unit
		)
	]


def resolve_shortcut(unit: Union[str, Dict[str, Any]]) -> Dict[str, Any]:
	return unit if isinstance(unit, dict) else {'unit': unit}
