#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/config/builder/TierConfigCollector.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 16.10.2019
# Last Modified Date: 25.10.2019
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Dict
from ampel.logging.AmpelLogger import AmpelLogger
from ampel.config.collector.ConfigCollector import ConfigCollector


class TierConfigCollector(ConfigCollector, abstract=True):
	"""
	"""

	def __init__(
		self, tier: int, conf_section: str, content: Dict = None, 
		logger: AmpelLogger = None, verbose: bool = False
	):
		""" """
		super().__init__(conf_section, content, logger, verbose)
		self.tier = tier
