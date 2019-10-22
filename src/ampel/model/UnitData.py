#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/model/UnitData.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 26.09.2019
# Last Modified Date: 10.10.2019
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from pydantic import validator
from typing import Dict, Union, Tuple, Optional
from ampel.common.docstringutils import gendocstring
from ampel.model.AmpelBaseModel import AmpelBaseModel


@gendocstring
class UnitData(AmpelBaseModel):
	"""
	run_config types:
	* None -> no run config
	* str -> a corresponding alias key must match the provided string
	* int -> should not be used (set internally for T2 units). \
	a corresponding runConfig key must match the provided integer
	* Dict -> run config parameters are provided directly
	"""
	unit_id: str
	init_config: Union[None, int, str, Dict] = None
	run_config: Union[None, int, str, Dict] = None
	resources: Optional[Tuple[str]] = None
	override: Optional[Dict] = None

	@validator('resources', pre=True, whole=True)
	def validate_resources(cls, resources):
		""" """
		# cast to sequence
		if isinstance(resources, str):
			return (resources,)

		return resources
