#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/template/ChannelWithProcsTemplate.py
# License:             BSD-3-Clause
# Author:              valery brinnel <firstname.lastname@gmail.com>
# Date:                16.10.2019
# Last Modified Date:  05.01.2022
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

from ampel.log.AmpelLogger import AmpelLogger
from typing import Any
from ampel.config.builder.FirstPassConfig import FirstPassConfig
from ampel.abstract.AbsChannelTemplate import AbsChannelTemplate
from ampel.model.ChannelModel import ChannelModel

class ChannelWithProcsTemplate(AbsChannelTemplate):
	""" Convenience class allowing channel definitions to include processes.  """

	# Note: not using list[ProcessModel] on purpose since embedded processes
	# might need template processing as well
	process: list[dict[str, Any]]

	def get_channel(self, logger: AmpelLogger) -> dict[str, Any]:
		return self.dict(include=ChannelModel.get_model_keys())

	def get_processes(self, logger: AmpelLogger, first_pass_config: FirstPassConfig) -> list[dict[str, Any]]:

		# Note: not enforcing channel selection for t3 processes
		# as these could require template processing first

		return [
			self.transfer_channel_parameters(p)
			for p in self.process
		]
