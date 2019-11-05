#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/config/builder/collector/ProcessConfigCollector.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 16.10.2019
# Last Modified Date: 25.10.2019
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Dict, Any
from ampel.config.collector.TierConfigCollector import TierConfigCollector


class ProcessConfigCollector(TierConfigCollector):
	"""
	"""

	# pylint: disable=inconsistent-return-statements
	def add(self, arg: Dict[str, Any], dist_name: str = None) -> None:
		""" """

		# Doing basic validation here already
		for k in ("processName", "schedule"):
			if k not in arg:
				return self.missing_key("Process", k, dist_name)

		proc_name = arg["processName"]

		if 'tier' not in arg:
			arg['tier'] = self.tier

		if dist_name:
			arg['distName'] = dist_name

		if self.verbose:
			self.logger.verbose(
				f"-> Adding t{arg['tier']} process: {proc_name}"
			)

		if self.get(proc_name):
			return self.duplicated_entry(
				section_detail=f"t{arg['tier']}.process", 
				conf_key=proc_name, 
				prev_dist=self.get(proc_name).get('distName'),
				new_dist=dist_name if dist_name else arg.get('distName')
			)

		self.__setitem__(proc_name, arg)
