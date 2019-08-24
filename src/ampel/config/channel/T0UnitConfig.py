#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/config/channel/T0UnitConfig.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 02.09.2018
# Last Modified Date: 17.09.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import pkg_resources
from typing import Dict, Any, Union
from pydantic import BaseModel, validator
from ampel.common.docstringutils import gendocstring
from ampel.config.AmpelConfig import AmpelConfig
from ampel.config.AmpelModelExtension import AmpelModelExtension
from ampel.common.AmpelUnitLoader import AmpelUnitLoader
from ampel.config.EncryptedConfig import EncryptedConfig

@gendocstring
class T0UnitConfig(AmpelModelExtension):
	"""
	Config holder for T0 units (filters)
	"""

	unitId: str
	runConfig: Union[None, Dict] = None


	@validator('unitId', pre=True)
	def validate_units(cls, value):
		""" """

		if (
			"t0" in AmpelConfig._ignore_unavailable_units or 
			value in AmpelConfig._ignore_unavailable_units
		):
			return value

		if next(
			pkg_resources.iter_entry_points('ampel.t0.units', value), 
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

	@validator('runConfig')
	def validate_run_config(cls, run_config, values, **kwargs):
		"""
		"""

		# Exists (checked by prior validator)
		klass = AmpelUnitLoader.get_class(
			tier=0, unit_name=values['unitId']
		)

		if hasattr(klass, 'RunConfig'):

			RunConfig = getattr(klass, 'RunConfig')
			rc = RunConfig(**run_config)

			for k in rc.fields.keys():
				v = getattr(rc, k)
				if type(v) is EncryptedConfig:
					setattr(rc, k, AmpelConfig.decrypt_config(v.dict()))

			return rc

		return run_config
