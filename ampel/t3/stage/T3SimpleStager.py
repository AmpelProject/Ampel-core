#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/t3/stage/T3SimpleStager.py
# License:             BSD-3-Clause
# Author:              valery brinnel <firstname.lastname@gmail.com>
# Date:                18.04.2021
# Last Modified Date:  17.12.2021
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

from collections.abc import Generator

from ampel.abstract.AbsT3Unit import AbsT3Unit
from ampel.content.T3Document import T3Document
from ampel.model.UnitModel import UnitModel
from ampel.struct.AmpelBuffer import AmpelBuffer
from ampel.struct.T3Store import T3Store
from ampel.t3.stage.SimpleViewGenerator import SimpleViewGenerator
from ampel.t3.stage.T3ThreadedStager import T3ThreadedStager
from ampel.types import Annotated, OneOrMany


class T3SimpleStager(T3ThreadedStager):
	"""
	To be used in combination with MongoViews.
	Otherwise, use T3ProjectingStager.
	"""

	#: t3 units (AbsT3Unit) to execute
	execute: OneOrMany[Annotated[UnitModel, AbsT3Unit]]

	def __init__(self, **kwargs) -> None:

		super().__init__(**kwargs)
		self.units: list[AbsT3Unit] = []

		if self.logger.verbose > 1:
			self.logger.debug("Setting up T3SimpleStager")

		for exec_def in [self.execute] if isinstance(self.execute, UnitModel) else self.execute:
			self.units.append(self.get_unit(exec_def))


	def stage(self,
		gen: Generator[AmpelBuffer, None, None],
		t3s: T3Store
	) -> None | Generator[T3Document, None, None]:

		if len(self.units) == 1:
			return self.proceed(
				self.units[0],
				SimpleViewGenerator(self.units[0], gen, self.stock_updr, self.context.config),
				t3s
			)

		return self.proceed_threaded(self.units, gen, t3s)
