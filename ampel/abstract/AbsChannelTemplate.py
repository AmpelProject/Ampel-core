#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/abstract/AbsChannelTemplate.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 27.10.2019
# Last Modified Date: 21.03.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from logging import Logger
from typing import List, Dict, Any, Optional
from ampel.abc import abstractmethod
from ampel.abc.AmpelABC import AmpelABC
from ampel.model.ChannelModel import ChannelModel

class AbsChannelTemplate(AmpelABC, ChannelModel, abstract=True):

	template: Optional[str]

	@abstractmethod
	def get_channel(self, logger: Logger) -> Dict[str, Any]:
		...

	@abstractmethod
	def get_processes(self, units_config: Dict[str, Any], logger: Logger) -> List[Dict[str, Any]]:
		...
