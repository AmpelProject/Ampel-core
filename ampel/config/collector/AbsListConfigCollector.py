#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/config/collector/AbsListConfigCollector.py
# License:             BSD-3-Clause
# Author:              valery brinnel <firstname.lastname@gmail.com>
# Date:                03.03.2020
# Last Modified Date:  03.03.2020
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

from typing import Any, List, Optional, Union
from ampel.base.decorator import abstractmethod
from ampel.base.AmpelABC import AmpelABC
from ampel.config.collector.ConfigCollector import ConfigCollector


class AbsListConfigCollector(ConfigCollector, AmpelABC, abstract=True):

	@abstractmethod
	def add(self,
		arg: List[Any],
		dist_name: str,
		version: Union[str, float, int],
		register_file: str,
	) -> Optional[int]:
		...
