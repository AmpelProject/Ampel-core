#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/config/channel/T0UnitConfig.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 02.09.2018
# Last Modified Date: 17.09.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import pkg_resources
from typing import Dict, Any, Union
from pydantic import BaseModel, validator
from ampel.pipeline.common.docstringutils import gendocstring
from ampel.pipeline.config.AmpelConfig import AmpelConfig
from ampel.pipeline.config.AmpelModelExtension import AmpelModelExtension

@gendocstring
class T0UnitConfig(BaseModel, AmpelModelExtension):
	"""
	Config holder for T0 units (filters)
	"""

	unitId: str
	runConfig: Union[None, Dict[str, Any]] = None


	@validator('unitId', pre=True)
	def validate_units(cls, value):
		""" """

		if (
			"t0" in AmpelConfig._ignore_unavailable_units or 
			value in AmpelConfig._ignore_unavailable_units
		):
			return value

		if next(
			pkg_resources.iter_entry_points('ampel.pipeline.t0.units', value), 
			None
		) is None:
			cls.print_and_raise(
				header="t0Filter->unitId config error",
				msg="Unknown T0 unit: '%s'\n" % value +
					"Please either install the corresponding Ampel plugin\n" +
					"or explicitely ignore unavailable T0 units by calling\n" + 
					"AmpelConfig.ignore_unavailable_units('t0')\n" +
					"or alternatively load ampel defaults using:\n" +
					"AmpelConfig.load_defaults(ignore_unavailable_units=['t0'])"
			)

		return value
