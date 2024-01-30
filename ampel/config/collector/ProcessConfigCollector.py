#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/config/collector/ProcessConfigCollector.py
# License:             BSD-3-Clause
# Author:              valery brinnel <firstname.lastname@gmail.com>
# Date:                16.10.2019
# Last Modified Date:  02.01.2023
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

from typing import Any

from ampel.config.collector.AbsDictConfigCollector import AbsDictConfigCollector
from ampel.log import VERBOSE


class ProcessConfigCollector(AbsDictConfigCollector):

	def add(self,
		arg: dict[str, Any],
		dist_name: str,
		version: str | float | int,
		register_file: str
	) -> None:

		# Doing basic validation here already
		for k in ("name", "schedule"):
			if k not in arg:
				return self.missing_key(
					what="Process", key=k, dist_name=dist_name, register_file=register_file
				)

		proc_name = arg["name"]

		if 'tier' not in arg:
			if len(self.tier) == 2 and self.tier.startswith('t') and self.tier[1].isdigit():
				arg['tier'] = int(self.tier[1])
			else:
				arg['tier'] = None

		if self.verbose:
			self.logger.log(
				VERBOSE, f"Adding {self.tier} process: '{proc_name}' from {register_file}"
			)

		if not self.check_duplicates(
			proc_name, dist_name, version, register_file,
			section_detail = f"{arg['tier']}.process"
		):
			self.__setitem__(proc_name, arg)
		
		return None
