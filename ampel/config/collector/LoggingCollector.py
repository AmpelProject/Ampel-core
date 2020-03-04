#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/config/collector/LoggingCollector.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 03.03.2020
# Last Modified Date: 03.03.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Dict, Any, Optional, Literal
from ampel.config.collector.AbsDictConfigCollector import AbsDictConfigCollector
from ampel.config.collector.ConfigCollector import ConfigCollector
from ampel.model.AmpelStrictModel import AmpelStrictModel


class LoggingConfModel(AmpelStrictModel):
	aggregate_interval: float = 1.0
	console_logging_option: Literal["default", "compact", "compact", "headerless"] = "default"
	default_stream: Literal["stdout", "stderr"] = "stdout"
	flush_len: int = 1000
	col_name: str = "logs"


class LoggingCollector(AbsDictConfigCollector):

	def add(self,
		arg: Dict[str, Any],
		file_name: Optional[str] = None,
		dist_name: Optional[str] = None
	) -> None:
		""" """
		# validate model
		if self.verbose:
			self.logger.verbose("Validating logging configuration")

		try:

			cm = LoggingConfModel(**arg)

			for k, v in cm.dict().items():
				self.__setitem__(k, v)

		except Exception:
			self.error(
				"Incorrect logging configuration " +
				ConfigCollector.distrib_hint(file_name, dist_name)
			)
