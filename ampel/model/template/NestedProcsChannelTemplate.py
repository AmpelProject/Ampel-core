#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/model/template/NestedProcsChannelTemplate.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 16.10.2019
# Last Modified Date: 28.10.2019
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from logging import Logger
from typing import List, Dict, Any
from ampel.model.ProcessModel import ProcessModel
from ampel.abstract.AbsChannelTemplate import AbsChannelTemplate


class NestedProcsChannelTemplate(AbsChannelTemplate):
	"""
	Convenience class which allows channel definitions to include processes.
	"""
	#template: str = "default"
	process: List[ProcessModel]


	def get_channel(self, logger: Logger) -> Dict[str, Any]:

		return {
			"channel": self.channel,
			"distrib": self.distrib,
			"active": self.active,
			"access": self.access,
			"contact": self.contact,
			"policy": self.policy
		}


	def get_processes(self, units_config: Dict[str, Any], logger: Logger) -> Dict[str, Any]:
		""" """
		# pylint: disable=no-member
		for p in self.process:

			if p.tier == 3:

				try:

					# Enforce channel criteria for "stock" (transients) selection
					p.processor.config.select.config.channels = self.channel
					logger.info(
						f" -> {self.channel} channel selection criteria "
						f"applied to process {p.name}"
					)

				except Exception as e:

					logger.error(
						f" -> Warning: cannot enforce channel selection "
						f"criteria in process {p.name}",
						exc_info=e
					)

					raise ValueError(
						f"Invalid process {p.name} embedded in "
						f"channel {self.channel}"
					)

			p.dist_name = self.distrib

		return [
			p.dict(skip_defaults=True, by_alias=True)
			for p in self.process
		]
