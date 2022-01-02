#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/config/collector/LoggingCollector.py
# License:             BSD-3-Clause
# Author:              valery brinnel <firstname.lastname@gmail.com>
# Date:                03.03.2020
# Last Modified Date:  18.12.2021
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

from typing import Any
from ampel.config.collector.AbsDictConfigCollector import AbsDictConfigCollector
from ampel.config.collector.ConfigCollector import ConfigCollector
from ampel.log.handlers.AmpelStreamHandler import AmpelStreamHandler
from ampel.mongo.update.var.DBLoggingHandler import DBLoggingHandler
from ampel.log import VERBOSE


class LoggingCollector(AbsDictConfigCollector):

	def add(self,
		arg: dict[str, Any],
		dist_name: str,
		version: str | float | int,
		register_file: str
	) -> None:

		# validate model
		if self.verbose:
			self.logger.log(VERBOSE, "Validating logging configuration")

		try:

			# Loop through logging profiles
			for profile, config in arg.items():

				if "console" in config:
					try:
						AmpelStreamHandler(**config['console'])
					except Exception as e:
						self.error(
							f"Incorrect console logging configuration for: {config['console']} " +
							ConfigCollector.distrib_hint(dist_name, register_file) + ": \n" + str(e)
						)
						continue

				if "db" in config:
					try:
						DBLoggingHandler.validate(config['db'])
					except Exception as e:
						self.error(
							f"Incorrect db logging configuration for: {config['db']} " +
							ConfigCollector.distrib_hint(dist_name, register_file) + ": \n" + str(e)
						)
						continue

				self.__setitem__(profile, config)

				if self.verbose:
					self.logger.log(VERBOSE, f"Adding logging profile '{profile}'")

		except Exception as e:
			print(e)
			self.error(
				"Incorrect logging configuration " +
				ConfigCollector.distrib_hint(dist_name, register_file)
			)
