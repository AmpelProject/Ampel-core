#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/t3/run/T3ChannelUnitRunner.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 21.06.2020
# Last Modified Date: 21.06.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Sequence
from ampel.type import ChannelId
from ampel.core.AmpelBuffer import AmpelBuffer
from ampel.core.AmpelContext import AmpelContext
from ampel.model.UnitModel import UnitModel
from ampel.t3.run.AbsT3UnitRunner import AbsT3UnitRunner
from ampel.t3.run.T3UnitRunner import T3UnitRunner


class T3ChannelUnitRunner(AbsT3UnitRunner):
	"""
	UnitRunner class that configures the underlying T3UnitRunner instance
	to filter and project AmpelBuffer instances with respect to a single channel.
	Essentially a shortcut class since the same functionality
	can be obtained using a properly configured T3UnitRunner instance.
	T3UnitRunner is not inherited because of the differences in model definitions.
	"""

	channel: ChannelId
	execute: Sequence[UnitModel]


	def __init__(self, context: AmpelContext, **kwargs) -> None:
		"""
		:param channel: channel name/id
		:param execute: sequence of T3 units to be executed
		"""

		self._unit_runner = T3UnitRunner(
			context = context,
			directives=[
				T3UnitRunner.RunDirective(
					filter = UnitModel(
						unit="T3AmpelBufferFilter",
						config={"channel": kwargs['channel']}
					),
					project = UnitModel(
						unit = "T3ChannelProjector",
						config = {"channel": kwargs['channel']}
					),
					execute = kwargs.pop('execute')
				)
			],
			**kwargs
		)


	def run(self, data: Sequence[AmpelBuffer]) -> None:
		self._unit_runner.run(data)

	def done(self) -> None:
		self._unit_runner.done()
