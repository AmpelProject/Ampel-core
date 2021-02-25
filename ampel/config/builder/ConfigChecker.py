#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/config/builder/ConfigChecker.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 03.09.2019
# Last Modified Date: 25.02.2021
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>


from typing import Dict, Any
from ampel.log.AmpelLogger import AmpelLogger, DEBUG, ERROR
from ampel.core.UnitLoader import UnitLoader
from ampel.config.AmpelConfig import AmpelConfig
from ampel.dev.DictSecretProvider import PotemkinSecretProvider


class ConfigChecker:

	def __init__(self, logger: AmpelLogger = None, verbose: bool = False):

		self.verbose = verbose
		self.logger = AmpelLogger.get_logger(
			console={'level': DEBUG if verbose else ERROR}
		) if logger is None else logger


	def validate(self, config: Dict[str, Any]) -> Dict[str, Any]:
		"""
		:returns config if check passed
		:raises: BadConfig
		"""

		unit_loader = UnitLoader(
			AmpelConfig(config),
			secrets=PotemkinSecretProvider(),
		)

		# Recursively load all UnitModels

		# Load all Process models

		return config
