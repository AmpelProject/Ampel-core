#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/config/collector/ResourceConfigCollector.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 16.10.2019
# Last Modified Date: 22.04.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Dict, Any, Optional
from ampel.log.AmpelLogger import AmpelLogger, VERBOSE
from ampel.config.collector.AbsDictConfigCollector import AbsDictConfigCollector


class ResourceConfigCollector(AbsDictConfigCollector):

	def __init__(self,
		conf_section: str, content: Optional[Dict] = None,
		logger: Optional[AmpelLogger] = None, verbose: bool = False
	):
		super().__init__(conf_section, content, logger, verbose)

		# Used to temporarily save distribution/source conf information of aliases
		# (usefuly in case of conflicts)
		self.tmp_resource: Dict[str, Any] = {}


	def add(self,
		arg: Dict[str, Any], file_name: Optional[str] = None,
		dist_name: Optional[str] = None
	) -> None:
		""" """

		if not isinstance(arg, dict):
			self.error(
				f"Resource value must be a dict. "
				f"Offending value {arg}\n"
				f"{self.distrib_hint(file_name, dist_name)}"
			)
			return

		for k, v in arg.items():

			try:

				key = k

				# Global resource
				if k and k[0] == "%":
					key = k[1:]
					scope = "global"
				else:
					# Distribution scoped alias
					if dist_name:
						key = f"{dist_name}/{k}"
						scope = "scoped"
					else:
						scope = ""

				self.tmp_resource[key] = file_name, dist_name

				if self.verbose:
					self.logger.log(VERBOSE,
						f"Adding {scope} resource '{k}' " +
						f"from file {file_name}" if file_name else ""
					)

				if self.get(key):
					self.duplicated_entry(
						conf_key = key,
						section_detail = f"{scope} resource",
						new_file = file_name,
						new_dist = dist_name,
						prev_file = self.tmp_resource.get(key, "unknown")[0],
						prev_dist = self.tmp_resource.get(key, "unknown")[1]
					)
					continue

				self.__setitem__(key, v)

			except Exception as e:
				self.error(
					f"Error occured while loading resource {k} " +
					self.distrib_hint(file_name, dist_name),
					exc_info=e
				)
