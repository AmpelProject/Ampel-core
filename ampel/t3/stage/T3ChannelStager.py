#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/t3/stage/T3ChannelStager.py
# License:             BSD-3-Clause
# Author:              valery brinnel <firstname.lastname@gmail.com>
# Date:                21.06.2020
# Last Modified Date:  10.12.2021
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

from collections.abc import Generator, Sequence
from ampel.types import ChannelId
from ampel.view.T3Store import T3Store
from ampel.struct.AmpelBuffer import AmpelBuffer
from ampel.core.AmpelContext import AmpelContext
from ampel.content.T3Document import T3Document
from ampel.model.UnitModel import UnitModel
from ampel.model.t3.T3ProjectionDirective import T3ProjectionDirective
from ampel.abstract.AbsT3Stager import AbsT3Stager
from ampel.t3.stage.T3ProjectingStager import T3ProjectingStager


class T3ChannelStager(AbsT3Stager):
	"""
	Stager class that configures an underlying T3ProjectingStager instance
	to filter and project AmpelBuffer instances with respect to a single channel.
	Essentially a shortcut class since the same functionality
	can be obtained using a properly configured T3ProjectingStager instance.
	T3ProjectingStager is not inherited because of the differences in model definitions.
	"""

	channel: ChannelId
	execute: Sequence[UnitModel]


	def __init__(self, context: AmpelContext, **kwargs) -> None:
		"""
		:param channel: channel name/id
		:param execute: sequence of T3 units to be executed
		"""

		self._stager = T3ProjectingStager(
			context = context,
			directives=[
				T3ProjectionDirective(
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


	def stage(self,
		gen: Generator[AmpelBuffer, None, None],
		t3s: T3Store
	) -> None | Generator[T3Document, None, None]:
		return self._stager.stage(gen, t3s)
