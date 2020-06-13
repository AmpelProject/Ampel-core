#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/config/collector/LoggingCollector.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 03.03.2020
# Last Modified Date: 12.06.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Dict, Any, Optional
from ampel.config.collector.AbsDictConfigCollector import AbsDictConfigCollector
from ampel.config.collector.ConfigCollector import ConfigCollector
from ampel.log.handlers.AmpelStreamHandler import AmpelStreamHandler
from ampel.log.handlers.DBLoggingHandler import DBLoggingHandler
from ampel.db.AmpelDB import AmpelDB
from ampel.log import VERBOSE


class FakeAmpelDB(AmpelDB):
	def __init__(self, **kwargs):
		pass
	def get_collection(self, arg):
		return None


class LoggingCollector(AbsDictConfigCollector):

	def add(self,
		arg: Dict[str, Any],
		file_name: Optional[str] = None,
		dist_name: Optional[str] = None
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
							ConfigCollector.distrib_hint(file_name, dist_name) +
							": \n" + str(e)
						)
						continue

				if "db" in config:
					try:
						DBLoggingHandler(FakeAmpelDB(), 12, **config['db'])
					except Exception as e:
						self.error(
							f"Incorrect db logging configuration for: {config['db']} " +
							ConfigCollector.distrib_hint(file_name, dist_name) +
							": \n" + str(e)
						)
						continue

				self.__setitem__(profile, config)

				if self.verbose:
					self.logger.log(VERBOSE, f"Adding logging profile '{profile}'")

		except Exception as e:
			print(e)
			self.error(
				"Incorrect logging configuration " +
				ConfigCollector.distrib_hint(file_name, dist_name)
			)
