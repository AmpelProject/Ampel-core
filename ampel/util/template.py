#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/util/template.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 16.07.2021
# Last Modified Date: 16.07.2021
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from importlib import import_module
from typing import List, Dict, Any, Union, Sequence
from ampel.model.UnitModel import UnitModel
from ampel.config.builder.FirstPassConfig import FirstPassConfig

'''
def check_tied_units(
	all_t2_units: List[UnitModel],
	config: Union[FirstPassConfig, Dict[str, Any]]
) -> None:
	"""
	:raises: ValueError if tied t2 units are present in t2_units but the required t2 units are not present in t2_compute
	"""
	all_units: List[str] = []
	tied_units: List[str] = []
	for el in all_t2_units:
		all_units.append(el.unit) # type: ignore
		if "AbsTiedT2Unit" in config['unit'][el.unit]['base']:
			tied_units.append(el.unit) # type: ignore
		
	s = set()
	for tied_unit in tied_units:
		klass = getattr(
			import_module(
				config['unit'][tied_unit]['fqn']
			),
			tied_unit
		)
		s.update(klass.get_tied_unit_names())

	if s.difference(all_units):
		raise ValueError(
			f"Following t2 unit(s) must be defined (required by "
			f"tied units): {list(s.difference(all_units))}"
		)
'''

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
