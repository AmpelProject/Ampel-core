#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/config/builder/collector/ResouceConfigCollector.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 16.10.2019
# Last Modified Date: 25.10.2019
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Dict, Any
from ampel.logging.AmpelLogger import AmpelLogger
from ampel.config.collector.ConfigCollector import ConfigCollector


class ResouceConfigCollector(ConfigCollector):
	"""
	"""

	def __init__(
		self, conf_section: str, content: Dict = None, 
		logger: AmpelLogger = None, verbose: bool = False
	):
		super().__init__(conf_section, content, logger, verbose)
		self.global_resource = {}


	def add(self, arg: Dict[str, Any], dist_name: str = None) -> None:
		""" """

		if not isinstance(arg, dict):
			self.error(
				f"Resource value must be a dict. "
				f"Offending value {arg}\n"
				f"{self.distrib_hint(dist_name)}" 
			)
			return

		for k, v in arg.items():

			try:

				key = k

				# Global resource
				if k and k[0] == "%":
					key = k[1:]
					scope = "global"
					self.global_resource[key] = dist_name
				else:
					# Distribution scoped alias
					if dist_name:
						key = f"{dist_name}/{k}"
						scope = "scoped"
					else:
						scope = ""

				if self.verbose:
					self.logger.verbose(f"-> Adding {scope} resource: {k}")

				if self.get(key):
					self.duplicated_entry(
						conf_key = key, section_detail = f"{scope} resource", 
						new_dist = dist_name, 
						prev_dist = dist_name if "/" in key else self.global_resource.get(key, "unknown")
					)
					continue

				self.__setitem__(key, v)

			except Exception as e:
				self.error(
					f"Error occured while loading resource {k} " +
					self.distrib_hint(dist_name),
					exc_info=e
				)
