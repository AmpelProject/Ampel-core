#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/config/channel/T2UnitConfig.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 02.09.2018
# Last Modified Date: 18.10.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import pkg_resources
from pydantic import validator
from ampel.pipeline.common.docstringutils import gendocstring
from ampel.pipeline.config.AmpelModelExtension import AmpelModelExtension
from ampel.pipeline.config.AmpelConfig import AmpelConfig


@gendocstring
class T2UnitConfig(AmpelModelExtension):
	"""
	Config holder for T2 units
	"""

	unitId: str
	upperLimits: bool = True
	runConfig: str = "default"


	@validator('unitId', pre=True)
	def validate_units(cls, value):
		""" """
		if (
			"t2" in AmpelConfig._ignore_unavailable_units or 
			value in AmpelConfig._ignore_unavailable_units
		):
			return value

		if next(
			pkg_resources.iter_entry_points('ampel.pipeline.t2.units', value), 
			None
		) is None:
			cls.print_and_raise(
				header="t2Compute->unitId config error",
				msg="Unknown T2 unit: '%s'\n" % value +
					"Please either install the corresponding Ampel plugin\n" +
					"or allow explicitely unavailable T2 units by calling\n" + 
					"AmpelConfig.ignore_unavailable_units('t2')\n" +
					"or alternatively load ampel defaults using:\n" +
					"AmpelConfig.load_defaults(ignore_unavailable_units=['t2'])"
			)

		return value
