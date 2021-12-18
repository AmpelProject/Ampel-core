#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/t3/stage/T3SimpleStager.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 18.04.2021
# Last Modified Date: 17.12.2021
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Optional, List, Generator

from ampel.types import OneOrMany, Annotated
from ampel.view.T3Store import T3Store
from ampel.model.UnitModel import UnitModel
from ampel.content.T3Document import T3Document
from ampel.abstract.AbsT3ReviewUnit import AbsT3ReviewUnit
from ampel.struct.AmpelBuffer import AmpelBuffer
from ampel.t3.stage.T3ThreadedStager import T3ThreadedStager
from ampel.t3.stage.SimpleViewGenerator import SimpleViewGenerator


class T3SimpleStager(T3ThreadedStager):
	"""
	To be used in combination with MongoViews.
	Otherwise, use T3ProjectingStager.
	"""

	#: t3 units (AbsT3ReviewUnit) to execute
	execute: OneOrMany[Annotated[UnitModel, AbsT3ReviewUnit]]

	def __init__(self, **kwargs) -> None:

		super().__init__(**kwargs)
		self.units: List[AbsT3ReviewUnit] = []

		if self.logger.verbose > 1:
			self.logger.debug("Setting up T3SimpleStager")

		for exec_def in [self.execute] if isinstance(self.execute, UnitModel) else self.execute:
			self.units.append(self.get_unit(exec_def))


	def stage(self,
		gen: Generator[AmpelBuffer, None, None],
		t3s: T3Store
	) -> Optional[Generator[T3Document, None, None]]:

		if len(self.units) == 1:
			return self.proceed(
				self.units[0],
				SimpleViewGenerator(self.units[0], gen, self.stock_updr),
				t3s
			)

		return self.proceed_threaded(self.units, gen, t3s)
