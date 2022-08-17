#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/t3/stage/NoViewGenerator.py
# License:             BSD-3-Clause
# Author:              valery brinnel <firstname.lastname@gmail.com>
# Date:                17.08.2022
# Last Modified Date:  17.08.2022
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

from typing import Iterable
from collections.abc import Generator
from ampel.abstract.AbsT3ReviewUnit import AbsT3ReviewUnit
from ampel.struct.AmpelBuffer import AmpelBuffer
from ampel.mongo.update.MongoStockUpdater import MongoStockUpdater
from ampel.t3.stage.BaseViewGenerator import BaseViewGenerator, T, T3Send


class NoViewGenerator(BaseViewGenerator[T]):

	def __init__(self,
		unit: AbsT3ReviewUnit,
		buffers: Iterable[AmpelBuffer],
		stock_updr: MongoStockUpdater
	) -> None:

		super().__init__(unit_name = unit.__class__.__name__, stock_updr = stock_updr)
		self.buffers = buffers
		self.View = unit._View

	def __iter__(self) -> Generator[T, T3Send, None]:
		for ab in self.buffers:
			yield ab # type: ignore
