#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/abstract/AbsProcessorTemplate.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 16.07.2021
# Last Modified Date: 16.07.2021
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Dict, Any
from ampel.log.AmpelLogger import AmpelLogger
from ampel.base.decorator import abstractmethod
from ampel.model.UnitModel import UnitModel
from ampel.config.builder.AbsConfigTemplate import AbsConfigTemplate


class AbsProcessorTemplate(AbsConfigTemplate, abstract=True):

	@abstractmethod
	def get_model(self, config: Dict[str, Any], logger: AmpelLogger) -> UnitModel:
		...
