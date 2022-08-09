#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/t3/stage/T3BaseStager.py
# License:             BSD-3-Clause
# Author:              valery brinnel <firstname.lastname@gmail.com>
# Date:                08.12.2021
# Last Modified Date:  19.12.2021
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

from time import time
from collections.abc import Generator

from ampel.types import ChannelId
from ampel.view.T3Store import T3Store
from ampel.model.UnitModel import UnitModel
from ampel.t3.T3DocBuilder import T3DocBuilder
from ampel.content.T3Document import T3Document
from ampel.abstract.AbsT3Stager import AbsT3Stager
from ampel.abstract.AbsT3ReviewUnit import AbsT3ReviewUnit, T
from ampel.t3.stage.BaseViewGenerator import BaseViewGenerator


class T3BaseStager(AbsT3Stager, T3DocBuilder, abstract=True):
	"""
	Base class for several stagers provided by ampel.
	This class does not implement the method stage(...) required by AbsT3Stager,
	it is up to the subclass to do it according to requirements.
	"""

	# Require single channel for now (T3DocBuilder allows multi-channel)
	channel: None | ChannelId = None


	def get_unit(self, unit_model: UnitModel, chan: None | ChannelId = None) -> AbsT3ReviewUnit:
		return self.context.loader.new_safe_logical_unit(
			unit_model,
			unit_type = AbsT3ReviewUnit,
			logger = self.logger,
			_chan = self.channel or chan
		)


	def proceed(self,
		t3_unit: AbsT3ReviewUnit,
		view_generator: BaseViewGenerator[T],
		t3s: T3Store
	) -> Generator[T3Document, None, None]:
		"""
		Executes the method 'process' of t3 unit with provided views generator and t3 store,
		handle potential t3 unit result
		"""

		ts = time()

		try:
			self.logger.info("Running T3 unit", extra={'unit': t3_unit.__class__.__name__})
			if (ret := t3_unit.process(view_generator, t3s)) or self.save_stock_ids:
				if x := self.handle_t3_result(t3_unit, ret, t3s, view_generator.get_stock_ids(), ts):
					yield x

		except Exception as e:
			self.event_hdlr.handle_error(e, self.logger)
		finally:
			self.flush(t3_unit)
