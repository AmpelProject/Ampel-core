#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/model/DataUnitModel.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 26.09.2019
# Last Modified Date: 06.03.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from pydantic import validator
from typing import Tuple, Optional
from ampel.model.PlainUnitModel import PlainUnitModel


class DataUnitModel(PlainUnitModel):
	"""
	Adds 'resource' definition to standard PlainUnitModel
	(which contains 'unit' and 'config' definitions)
	"""

	resource: Optional[Tuple[str, ...]]

	@validator('resource', pre=True, whole=True)
	def validate_resources(cls, resources):
		if isinstance(resources, str):
			return (resources, ) # cast to tuple/sequence

		return resources
