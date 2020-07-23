#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/abstract/AbsProcessController.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 06.04.2020
# Last Modified Date: 17.04.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import yaml
from typing import Dict, Optional, Literal, Sequence
from ampel.base import abstractmethod
from ampel.base.AmpelABC import AmpelABC
from ampel.config.AmpelConfig import AmpelConfig
from ampel.model.ProcessModel import ProcessModel
from ampel.log.AmpelLogger import AmpelLogger


class AbsProcessController(AmpelABC, abstract=True):


	@classmethod
	def new(cls,
		tier: Literal[0, 1, 2, 3],
		config_file_path: str,
		match: Optional[Sequence[str]] = None,
		exclude: Optional[Sequence[str]] = None,
		override: Optional[Dict] = None,
		verbose: int = 0,
		**kwargs
	):

		if config_file_path:
			with open(config_file_path, "r") as conf_file:
				config = AmpelConfig(yaml.load(conf_file), freeze=False)
		else:
			from ampel.core.AmpelContext import AmpelContext
			config = AmpelContext.build(tier=tier, freeze_config=False).config

		# Avoid circular imports
		from ampel.core.AmpelController import AmpelController
		proc_models = AmpelController.get_processes(
			config, tier=tier, match=match, exclude=exclude, controllers=[cls.__name__],
			logger=AmpelLogger.get_logger() if verbose else None, verbose=verbose
		)

		return cls(config, proc_models, verbose)


	def __init__(self, config: AmpelConfig, processes: Sequence[ProcessModel], verbose: int = 0) -> None:

		self.config = config
		self.proc_models = processes
		self.verbose = verbose


	@abstractmethod
	def schedule_processes(self):
		...
