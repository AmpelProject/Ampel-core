#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/t3/stage/T3BaseStager.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 08.12.2021
# Last Modified Date: 10.12.2021
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from time import time
from typing import Optional, Generator

from ampel.types import ChannelId
from ampel.view.T3Store import T3Store
from ampel.model.UnitModel import UnitModel
from ampel.t3.T3Writer import T3Writer
from ampel.t3.T3Processor import T3Processor
from ampel.content.T3Document import T3Document
from ampel.abstract.AbsT3Stager import AbsT3Stager
from ampel.abstract.AbsT3StageUnit import AbsT3StageUnit, T
from ampel.t3.stage.BaseViewGenerator import BaseViewGenerator


class T3BaseStager(AbsT3Stager, T3Writer, abstract=True):
	"""
	Base class for several stagers provided by ampel.
	This class does not implement the method stage(...) required by AbsT3Stager,
	it is up to the subclass to do it according to requirements.
	"""

	def get_unit(self, unit_model: UnitModel, chan: Optional[ChannelId] = None) -> AbsT3StageUnit:
		return T3Processor.spawn_logical_unit(
			unit_model,
			unit_type = AbsT3StageUnit,
			loader = self.context.loader,
			logger = self.logger,
			chan = self.channel or chan
		)


	def proceed(self,
		t3_unit: AbsT3StageUnit,
		view_generator: BaseViewGenerator[T],
		t3s: Optional[T3Store] = None
	) -> Generator[T3Document, None, None]:
		"""
		Executes the method 'process' of t3 unit with provided views generator and t3 store,
		handle potential t3 unit result
		"""

		ts = time()

		try:
			self.logger.info("Running T3unit", extra={'unit': t3_unit.__class__.__name__})
			if (ret := t3_unit.process(view_generator, t3s)) or self.save_stock_ids:
				if x := self.handle_t3_result(t3_unit, ret, view_generator.get_stock_ids(), ts):
					yield x

		except Exception as e:
			self.handle_error(e)
		finally:
			self.flush(t3_unit)
