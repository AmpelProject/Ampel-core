#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/t3/stage/T3SimpleStager.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 18.04.2021
# Last Modified Date: 22.04.2021
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Sequence, List, Generator, Optional, Union
from ampel.struct.AmpelBuffer import AmpelBuffer
from ampel.model.UnitModel import UnitModel
from ampel.content.T3Document import T3Document
from ampel.abstract.AbsT3Unit import AbsT3Unit
from ampel.t3.stage.T3BaseStager import T3BaseStager
from ampel.t3.stage.SimpleViewGenerator import SimpleViewGenerator


class T3SimpleStager(T3BaseStager):
	"""
	To be used in combination with MongoViews.
	Otherwise, use T3ProjectingStager.
	"""

	#: t3 units (AbsT3Unit) to execute
	execute: Sequence[UnitModel]

	def __init__(self, **kwargs) -> None:

		super().__init__(**kwargs)
		self.units: List[AbsT3Unit] = []

		if self.logger.verbose > 1:
			self.logger.debug("Setting up T3SimpleStager")

		for exec_def in self.execute:
			self.units.append(self.get_unit(exec_def))


	def stage(self, data: Generator[AmpelBuffer, None, None]) -> Optional[Union[T3Document, List[T3Document]]]:

		if len(self.units) == 1:
			return self.supply(
				self.units[0],
				SimpleViewGenerator(self.units[0], data, self.stock_updr)
			)

		return self.multi_supply(self.units, data)
