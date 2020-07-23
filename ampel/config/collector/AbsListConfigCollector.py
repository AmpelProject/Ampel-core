#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/config/collector/AbsListConfigCollector.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 03.03.2020
# Last Modified Date: 03.03.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Any, List, Optional
from ampel.base import abstractmethod
from ampel.base.AmpelABC import AmpelABC
from ampel.config.collector.ConfigCollector import ConfigCollector


class AbsListConfigCollector(ConfigCollector, AmpelABC, abstract=True):

	@abstractmethod
	def add(self,
		arg: List[Any],
		file_name: Optional[str] = None,
		dist_name: Optional[str] = None
	) -> Optional[int]:
		...
