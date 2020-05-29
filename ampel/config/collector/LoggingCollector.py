#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/config/collector/LoggingCollector.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 03.03.2020
# Last Modified Date: 28.05.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from importlib import import_module
from typing import Dict, Any, Optional
from ampel.config.collector.AbsDictConfigCollector import AbsDictConfigCollector
from ampel.config.collector.ConfigCollector import ConfigCollector


class LoggingCollector(AbsDictConfigCollector):

	def add(self,
		arg: Dict[str, Any],
		file_name: Optional[str] = None,
		dist_name: Optional[str] = None
	) -> None:

		# validate model
		if self.verbose:
			self.logger.verbose("Validating logging configuration")

		try:

			# Loop through logger/handler class names
			for logger_class, profiles in arg.items():

				# Loop through logging profiles
				for profile, config in profiles.items():

					try:
						UnitClass = getattr(import_module(f"ampel.log.{logger_class}"), logger_class)
					except Exception:
						try:
							UnitClass = getattr(import_module(f"ampel.log.handlers.{logger_class}"), logger_class)
						except Exception:
							try: # dict key could also be a FQN
								UnitClass = getattr(import_module(f"{logger_class}"), logger_class.split('\n')[-1])
							except Exception:
								self.error(
									f"Unknown logger: {logger_class} " +
									ConfigCollector.distrib_hint(file_name, dist_name)
								)
								continue

					try:
						UnitClass(**config)
					except Exception as e:
						self.error(
							f"Incorrect logging configuration for {logger_class}: {config}" +
							ConfigCollector.distrib_hint(file_name, dist_name) +
							": \n" + str(e)
						)
						continue

					if logger_class in self:
						profiles = self.__getitem__(logger_class)
					else:
						profiles = {}
						self.__setitem__(logger_class, profiles)

					if self.verbose:
						self.logger.verbose(f"Adding logging profile '{profile}' associated with logger {logger_class}")

					profiles[profile] = config

		except Exception as e:
			print(e)
			self.error(
				"Incorrect logging configuration " +
				ConfigCollector.distrib_hint(file_name, dist_name)
			)
