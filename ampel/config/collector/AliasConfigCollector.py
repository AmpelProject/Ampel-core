#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/config/collector/AliasConfigCollector.py
# License:             BSD-3-Clause
# Author:              valery brinnel <firstname.lastname@gmail.com>
# Date:                16.10.2019
# Last Modified Date:  02.01.2023
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

from typing import Any

from ampel.config.collector.AbsDictConfigCollector import AbsDictConfigCollector
from ampel.log import VERBOSE


class AliasConfigCollector(AbsDictConfigCollector):
	"""
	Aliases are "tier" scoped and by default scoped
	at the (package) distribution level as well
	"""

	def __init__(self, **kwargs) -> None:
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
					key = f"{dist_name}/{k}"
					scope = "scoped"

				if self.verbose:
					self.logger.log(VERBOSE,
						f"Adding {scope} {self.tier} alias: {key}"
					)

				if self.check_duplicates(
					key, dist_name, version, register_file,
					section_detail = f"{self.tier} {scope} alias"
				):
					continue

				self.__setitem__(key, v)

			except Exception as e:
				self.error(
					f"Error occured while loading {self.tier} alias {key} " +
					self.distrib_hint(dist_name, register_file),
					exc_info=e
				)
