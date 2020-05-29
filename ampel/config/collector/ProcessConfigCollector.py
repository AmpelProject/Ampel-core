#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/config/collector/ProcessConfigCollector.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 16.10.2019
# Last Modified Date: 09.02.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Dict, Any, Optional
from ampel.config.collector.AbsDictConfigCollector import AbsDictConfigCollector


class ProcessConfigCollector(AbsDictConfigCollector):

	def add(self,
		arg: Dict[str, Any], file_name: Optional[str] = None,
		dist_name: Optional[str] = None
	) -> None:

		# Doing basic validation here already
		for k in ("name", "schedule"):
			if k not in arg:
				return self.missing_key("Process", k, file_name, dist_name)

		proc_name = arg["name"]

		if 'tier' not in arg:
			arg['tier'] = int(self.tier[1])

		if dist_name:
			arg['distrib'] = dist_name

		if file_name:
			arg['source'] = file_name

		if self.verbose:
			self.logger.verbose(
				f"Adding t{arg['tier']} process: '{proc_name}'" +
				f" from file '{file_name}'" if file_name else ""
			)

		if self.get(proc_name):
			return self.duplicated_entry(
				section_detail = f"{arg['tier']}.process",
				conf_key = proc_name, new_dist = dist_name, new_file = file_name
			)

		self.__setitem__(proc_name, arg)
