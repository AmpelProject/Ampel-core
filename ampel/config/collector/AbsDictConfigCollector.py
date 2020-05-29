#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/config/collector/AbsDictConfigCollector.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 03.03.2020
# Last Modified Date: 03.03.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Any, Optional, Dict
from ampel.base import abstractmethod
from ampel.base.AmpelABC import AmpelABC
from ampel.config.collector.ConfigCollector import ConfigCollector


class AbsDictConfigCollector(ConfigCollector, AmpelABC, abstract=True):

	@abstractmethod
	def add(self,
		arg: Dict[str, Any],
		file_name: Optional[str] = None,
		dist_name: Optional[str] = None
	) -> Optional[int]:
		...
