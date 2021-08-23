#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/util/template.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 16.07.2021
# Last Modified Date: 16.07.2021
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from importlib import import_module
from typing import List, Dict, Any, Union, Sequence, Optional
from ampel.model.UnitModel import UnitModel
from ampel.model.ingest.T2Compute import T2Compute
from ampel.config.builder.FirstPassConfig import FirstPassConfig


class T2UnitModel(T2Compute):
	config: Optional[Union[int, str, Dict[str, Any]]] # type: ignore


def check_tied_units(
		all_t2_units: List[T2UnitModel],
		first_pass_config: FirstPassConfig
	) -> None:
		"""
		:raises: ValueError if tied t2 units are present in t2_units but the requred t2 units are not present in t2_compute
		"""
		tied_units: List[T2UnitModel] = []
		for el in all_t2_units:
			if "AbsTiedT2Unit" in first_pass_config['unit'][el.unit]['base']:
				tied_units.append(el) # type: ignore
			
		s = set()
		for tied_unit in tied_units:
			klass = getattr(
				import_module(
					first_pass_config['unit'][tied_unit.unit]['fqn']
				),
				str(tied_unit.unit)
			)
			s.update(klass.get_tied_unit_names())

		if missing_deps := s.difference({u.unit for u in all_t2_units}):
			raise ValueError(
				f"Following t2 unit(s) must be defined (required by "
				f"tied units): {list(missing_deps)}"
			)
		
		def as_unitmodel(t2_unit_model: T2UnitModel) -> UnitModel:
			return UnitModel(**{k: v for k,v in t2_unit_model.dict().items() if k in UnitModel.__fields__})

		for tied_unit in tied_units:
			if (
				isinstance(tied_unit.config, dict)
				and (t2_deps := (tied_unit.config.get("t2_dependency") or []))
			):
				for t2_dep in t2_deps:
					dependency_config = UnitModel(unit=t2_dep["unit"], config=t2_dep.get("config"))
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
