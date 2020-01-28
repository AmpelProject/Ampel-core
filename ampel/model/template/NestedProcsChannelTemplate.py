#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/model/template/NestedProcsChannelTemplate.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 16.10.2019
# Last Modified Date: 28.10.2019
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import List, Dict, Any
from ampel.model.ProcessModel import ProcessModel
from logging import Logger
from ampel.abstract.AbsChannelTemplate import AbsChannelTemplate


class NestedProcsChannelTemplate(AbsChannelTemplate):
	""" 
	Convenience class which allows channel definitions to include processes.
	"""
	#template: str = "default"
	process: List[ProcessModel]


	def get_channel(self, logger: Logger) -> Dict[str, Any]:
		""" """

		# pylint: disable=no-member
		return {
		    "channel": self.channel,
    		"distName": self.distName,
			"active": self.active,
    		"access": self.access,
    		"contact": self.contact,
    		"policy": self.policy
		}


	def get_processes(self, logger: Logger) -> Dict[str, Any]:
		""" """
		# pylint: disable=no-member
		for p in self.process:

			if p.tier == 3:

				try:

					# Enforce channel criteria for "stock" (transients) selection 
					p.executor.initConfig.select.initConfig.channels = self.channel
					logger.info(
						f" -> {self.channel} channel selection criteria "
						f"applied to process {p.processName}"
					)

				except Exception as e:

					logger.error(
						f" -> Warning: cannot enforce channel selection "
						f"criteria in process {p.processName}",
						exc_info=e
					)

					raise ValueError(
						f"Invalid process {p.processName} embedded in "
						f"channel {self.channel}"
					)

			p.dist_name = self.distName

		return [
			p.dict(skip_defaults=True, by_alias=True) 
			for p in self.process
		]
