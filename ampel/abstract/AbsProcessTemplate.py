#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File:                ampel/abstract/AbsProcessTemplate.py
# License:             BSD-3-Clause
# Author:              valery brinnel <firstname.lastname@gmail.com>
# Date:                27.10.2019
# Last Modified Date:  27.10.2019
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

from typing import Dict, Any
from ampel.log.AmpelLogger import AmpelLogger
from ampel.base.decorator import abstractmethod
from ampel.config.builder.AbsConfigTemplate import AbsConfigTemplate


class AbsProcessTemplate(AbsConfigTemplate, abstract=True):

	@abstractmethod
	def get_process(self, config: Dict[str, Any], logger: AmpelLogger) -> Dict[str, Any]:
		...
