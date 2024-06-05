#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/t3/stage/NoViewGenerator.py
# License:             BSD-3-Clause
# Author:              valery brinnel <firstname.lastname@gmail.com>
# Date:                17.08.2022
# Last Modified Date:  17.08.2022
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

from collections.abc import Generator, Iterable

from ampel.abstract.AbsT3Unit import AbsT3Unit
from ampel.mongo.update.MongoStockUpdater import MongoStockUpdater
from ampel.struct.AmpelBuffer import AmpelBuffer
from ampel.t3.stage.BaseViewGenerator import BaseViewGenerator, T, T3Send


class NoViewGenerator(BaseViewGenerator[T]):

	def __init__(self,
		unit: AbsT3Unit,
		buffers: Iterable[AmpelBuffer],
		stock_updr: MongoStockUpdater
	) -> None:

		super().__init__(unit_name = unit.__class__.__name__, stock_updr = stock_updr)
		self.buffers = buffers
		self.View = unit._View  # noqa: SLF001

	def __iter__(self) -> Generator[T, T3Send, None]:
		yield from self.buffers # type: ignore[misc]