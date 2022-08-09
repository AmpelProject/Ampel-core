#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/abstract/AbsProcessorTemplate.py
# License:             BSD-3-Clause
# Author:              valery brinnel <firstname.lastname@gmail.com>
# Date:                16.07.2021
# Last Modified Date:  16.07.2021
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

from typing import Any
from ampel.log.AmpelLogger import AmpelLogger
from ampel.base.decorator import abstractmethod
from ampel.model.UnitModel import UnitModel
from ampel.config.builder.AbsConfigTemplate import AbsConfigTemplate


class AbsProcessorTemplate(AbsConfigTemplate, abstract=True):

	@abstractmethod
	def get_model(self, config: dict[str, Any], logger: AmpelLogger) -> UnitModel:
		...
