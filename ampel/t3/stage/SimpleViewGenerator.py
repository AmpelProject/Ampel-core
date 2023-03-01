#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/t3/stage/SimpleViewGenerator.py
# License:             BSD-3-Clause
# Author:              valery brinnel <firstname.lastname@gmail.com>
# Date:                20.04.2021
# Last Modified Date:  09.12.2021
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

from typing import Iterable
from collections.abc import Generator
from ampel.abstract.AbsT3ReviewUnit import AbsT3ReviewUnit
from ampel.config.AmpelConfig import AmpelConfig
from ampel.struct.AmpelBuffer import AmpelBuffer
from ampel.mongo.update.MongoStockUpdater import MongoStockUpdater
from ampel.t3.stage.BaseViewGenerator import BaseViewGenerator, T, T3Send


class SimpleViewGenerator(BaseViewGenerator[T]):

	def __init__(self,
		unit: AbsT3ReviewUnit,
		buffers: Iterable[AmpelBuffer],
		stock_updr: MongoStockUpdater,
		config: AmpelConfig,
	) -> None:

		super().__init__(unit_name = unit.__class__.__name__, stock_updr = stock_updr)
		self.buffers = buffers
		self.View = unit._View
		# ensure this generator's queue is consumed at most once 
		self._it = iter(self.buffers)
		self._config = config

	def __iter__(self) -> Generator[T, T3Send, None]:
		View = self.View
		l = self.stocks
		for ab in self._it:
			l.append(ab['id'])
			yield View.of(ab, self._config, freeze=True)
