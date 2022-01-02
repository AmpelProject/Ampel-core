#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/config/collector/AliasConfigCollector.py
# License:             BSD-3-Clause
# Author:              valery brinnel <firstname.lastname@gmail.com>
# Date:                16.10.2019
# Last Modified Date:  25.02.2021
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

from typing import Any
from ampel.config.collector.AbsDictConfigCollector import AbsDictConfigCollector
from ampel.log import VERBOSE


class AliasConfigCollector(AbsDictConfigCollector):
	"""
	Aliases are "tier" scoped and by default scoped
	at the (package) distribution level as well
	"""

	def __init__(self, **kwargs):
		super().__init__(**kwargs)
		self.global_alias: dict[str, Any] = {}


	def add(self,
		arg: dict[str, Any],
		dist_name: str,
		version: str | float | int,
		register_file: str
	) -> None:

		if not isinstance(arg, dict):
			self.error(
				f"{self.tier} alias value must be a dict. "
				f"Offending value {arg}\n"
				f"{self.distrib_hint(dist_name, register_file)}"
			)
			return

		for k, v in arg.items():

			if "/" in k:
				self.error(
					f"{self.tier} alias cannot contain '/'."
					f"Offending key {k}\n"
					f"{self.distrib_hint(dist_name, register_file)}"
				)
				continue

			try:

				key = k

				# Global alias
				if k and k[0] == "%":
					scope = "global"
					self.global_alias[key] = dist_name
				else:
					# Distribution scoped alias
					if dist_name:
						key = f"{dist_name}/{k}"
						scope = "scoped"
					else:
						scope = ""

				if self.verbose:
					self.logger.log(VERBOSE,
						f"Adding {scope} {self.tier} alias: {key}"
					)

				if self.get(key):
					self.duplicated_entry(
						conf_key = key,
						section_detail = f"{self.tier} {scope} alias",
						new_file = register_file,
						new_dist = dist_name,
						prev_file = self.get(key).get("conf", "unknown"), # type: ignore
						prev_dist = dist_name if "/" in key else self.global_alias.get(key, "unknown")
					)
					continue

				self.__setitem__(key, v)

			except Exception as e:
				self.error(
					f"Error occured while loading {self.tier} alias {key} " +
					self.distrib_hint(dist_name, register_file),
					exc_info=e
				)
