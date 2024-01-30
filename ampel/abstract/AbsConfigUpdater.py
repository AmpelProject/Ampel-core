#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/abstract/AbsConfigUpdater.py
# License:             BSD-3-Clause
# Author:              valery brinnel <firstname.lastname@gmail.com>
# Date:                05.04.2023
# Last Modified Date:  05.04.2023
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

from typing import Any

from ampel.base.AmpelABC import AmpelABC
from ampel.base.decorator import abstractmethod
from ampel.core.AmpelContext import AmpelContext
from ampel.log.AmpelLogger import AmpelLogger


class AbsConfigUpdater(AmpelABC, abstract=True):
	"""
	Context is required as alteration products might need to be registered in context sub-elements.
	Note that unlike AbsConfigMorpher, this abstract class is not a model (AmpelBaseModel).
	Implementing classes ex: HashT2Config, ResolveRunTimeAliases.
	"""

	@abstractmethod
	def alter(self, context: AmpelContext, target: dict[str, Any], logger: AmpelLogger) -> dict[str, Any]:
		...
