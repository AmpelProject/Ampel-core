#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/config/collector/LoggingCollector.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 03.03.2020
# Last Modified Date: 12.06.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Dict, Any, Union
from pydantic import validate_model
from ampel.config.collector.AbsDictConfigCollector import AbsDictConfigCollector
from ampel.config.collector.ConfigCollector import ConfigCollector
from ampel.log.handlers.AmpelStreamHandler import AmpelStreamHandler
from ampel.mongo.update.var.DBLoggingHandler import DBLoggingHandler
from ampel.log import VERBOSE


class LoggingCollector(AbsDictConfigCollector):

	def add(self,
		arg: Dict[str, Any],
		dist_name: str,
		version: Union[str, float, int],
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
						if DBLoggingHandler._model is None:
							DBLoggingHandler._create_model()
						validate_model(DBLoggingHandler._model, config['db'])
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
