#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/config/collector/ResourceConfigCollector.py
# License:             BSD-3-Clause
# Author:              valery brinnel <firstname.lastname@gmail.com>
# Date:                16.10.2019
# Last Modified Date:  02.01.2023
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

from typing import Any
from ampel.log.AmpelLogger import VERBOSE
from ampel.config.collector.AbsDictConfigCollector import AbsDictConfigCollector


class ResourceConfigCollector(AbsDictConfigCollector):

	def add(self,
		arg: dict[str, Any],
		dist_name: str,
		version: str | float | int,
		register_file: str
	) -> None:
		""" """

		if not isinstance(arg, dict):
			self.error(
				f"Resource value must be a dict. "
				f"Offending value {arg}\n"
				f"{self.distrib_hint(register_file, dist_name)}"
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

				if self.verbose:
					self.logger.log(VERBOSE,
						f"Adding {scope} resource '{k}' " +
						f"from file {register_file}" if register_file else ""
					)

				if self.check_duplicates(
					key, dist_name, version, register_file,
					section_detail = f"{scope} resource"
				):
					continue

				self.__setitem__(key, v)

			except Exception as e:
				self.error(
					f"Error occured while loading resource {k} " +
					self.distrib_hint(dist_name, register_file),
					exc_info = e
				)
