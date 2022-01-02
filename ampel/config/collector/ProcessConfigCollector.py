#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/config/collector/ProcessConfigCollector.py
# License:             BSD-3-Clause
# Author:              valery brinnel <firstname.lastname@gmail.com>
# Date:                16.10.2019
# Last Modified Date:  09.02.2020
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
				return self.missing_key(what="Process", key=k, dist_name=dist_name, register_file=register_file)

		proc_name = arg["name"]

		if 'tier' not in arg:
			if len(self.tier) == 2 and self.tier.startswith('t') and self.tier[1].isdigit():
				arg['tier'] = int(self.tier[1])
			else:
				arg['tier'] = None

		if dist_name:
			arg['distrib'] = dist_name

		if register_file:
			arg['source'] = register_file

		if self.verbose:
			self.logger.log(VERBOSE,
				f"Adding {self.tier} process: '{proc_name}'" +
				f" from file '{register_file}'" if register_file else ""
			)

		if self.get(proc_name):
			return self.duplicated_entry(
				section_detail = f"{arg['tier']}.process",
				conf_key = proc_name, new_dist = dist_name, new_file = register_file
			)

		self.__setitem__(proc_name, arg)
