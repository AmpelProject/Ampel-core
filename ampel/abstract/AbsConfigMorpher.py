#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/abstract/AbsConfigMorpher.py
# License:             BSD-3-Clause
# Author:              valery brinnel <firstname.lastname@gmail.com>
# Date:                27.10.2019
# Last Modified Date:  05.04.2023
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

from typing import Any

from ampel.base.AmpelABC import AmpelABC
from ampel.base.AmpelBaseModel import AmpelBaseModel
from ampel.base.decorator import abstractmethod
from ampel.log.AmpelLogger import AmpelLogger


class AbsConfigMorpher(AmpelABC, AmpelBaseModel, abstract=True):
	"""
	Template class aiming at forging processes conforming with ProcessModel
	"""

	template: None | str

	@abstractmethod
	def morph(self, ampel_config: dict[str, Any], logger: AmpelLogger) -> dict[str, Any]:
		...
