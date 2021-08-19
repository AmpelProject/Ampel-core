#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/abstract/AbsProcessController.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 06.04.2020
# Last Modified Date: 17.04.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Dict, Optional, Literal, Sequence, Any, ClassVar
from ampel.base.decorator import abstractmethod
from ampel.base.AmpelABC import AmpelABC
from ampel.base.AmpelBaseModel import AmpelBaseModel
from ampel.config.AmpelConfig import AmpelConfig
from ampel.secret.AmpelVault import AmpelVault
from ampel.model.ProcessModel import ProcessModel
from ampel.log.AmpelLogger import AmpelLogger
from ampel.metrics.AmpelMetricsRegistry import AmpelMetricsRegistry


class AbsProcessController(AmpelABC, AmpelBaseModel, abstract=True):

	config: AmpelConfig
	processes: Sequence[ProcessModel]
	vault: Optional[AmpelVault] = None
	log_profile: str = "default"

	process_count: ClassVar[Any] = AmpelMetricsRegistry.gauge(
		"processes",
		"Number of concurrent processes",
		subsystem=None,
		labelnames=("tier", "process")
	)
	process_exceptions: ClassVar[Any] = AmpelMetricsRegistry.counter(
		"process_exceptions",
		"Number of unhandled exceptions",
		subsystem=None,
		labelnames=("tier", "process")
	)

	@classmethod
	def new(cls,
		tier: Literal[0, 1, 2, 3],
		config_file_path: str,
		match: Optional[Sequence[str]] = None,
		exclude: Optional[Sequence[str]] = None,
		override: Optional[Dict] = None,
		log_profile: str = "default",
		**kwargs
	):
		from ampel.core.AmpelContext import AmpelContext
		if config_file_path:
			context = AmpelContext.load(config_file_path)
		else:
			context = AmpelContext.build(freeze_config=False)

		# Avoid circular imports
		from ampel.core.AmpelController import AmpelController
		proc_models = AmpelController.get_processes(
			context.config, tier=tier, match=match, exclude=exclude, controllers=[cls.__name__],
			logger=AmpelLogger.from_profile(context, log_profile)
		)

		return cls(config=context.config, processes=proc_models, log_profile=log_profile)


	@abstractmethod
	async def run(self) -> Any:
		"""
		Run this controller. This coroutine should not return until all its
		tasks have completed or it receives asyncio.CancelledError.
		"""
		...


	@abstractmethod
	def stop(self, name: Optional[str] = None) -> None:
		"""
		Gracefully stop processes.
		
		:param name: name of process to stop. If None, stop all processes.
		"""
		...

	def update(self,
		config: AmpelConfig,
		vault: Optional[AmpelVault],
		processes: Sequence[ProcessModel],
	) -> None:
		"""Change the configuration of the controller."""
		...
