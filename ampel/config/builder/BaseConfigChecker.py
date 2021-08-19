#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/config/builder/ConfigChecker.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 03.09.2019
# Last Modified Date: 26.02.2021
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>


import json
from typing import Any, Dict, Generator, Optional, Tuple

from ampel.log.AmpelLogger import AmpelLogger, DEBUG, ERROR
from ampel.config.AmpelConfig import AmpelConfig
from ampel.core.UnitLoader import UnitLoader
from ampel.secret.AmpelVault import AmpelVault
from ampel.secret.PotemkinSecretProvider import PotemkinSecretProvider


class BaseConfigChecker:
	"""
	Validates a config usually build by ConfigBuilder
	"""

	def __init__(self, config: Dict[str, Any], logger: Optional[AmpelLogger] = None, verbose: bool = False):

		self.verbose = verbose
		self.logger = AmpelLogger.get_logger(
			console={'level': DEBUG if verbose else ERROR}
		) if logger is None else logger

		self.loader = UnitLoader(
			AmpelConfig(config),
			db=None,
			provenance=False,
			vault=AmpelVault([PotemkinSecretProvider()])
		)

		# Fast deep copy and serialization check
		self.config = json.loads(json.dumps(config))


	def iter_procs(self,
		ignore_inactive: bool = False,
		raise_exc: bool = False
	) -> Generator[Tuple[str, str], None, None]:

		for tier in ("t0", "t1", "t2", "t3"):

			procs = list(self.config['process'][tier].keys())

			for proc in procs:

				if ignore_inactive and not self.config['process'][tier][proc].get('active', True):
					self.logger.info("Ignoring inactivated processor model", extra={"process": proc})
					continue

				yield (tier, proc)
