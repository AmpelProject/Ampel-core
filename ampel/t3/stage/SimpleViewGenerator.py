#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/t3/stage/SimpleViewGenerator.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 20.04.2021
# Last Modified Date: 20.04.2021
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Iterable, Generator
from ampel.abstract.AbsT3Unit import AbsT3Unit
from ampel.struct.JournalAttributes import JournalAttributes
from ampel.util.freeze import recursive_freeze
from ampel.struct.AmpelBuffer import AmpelBuffer
from ampel.core.StockJournalUpdater import StockJournalUpdater
from ampel.t3.stage.BaseViewGenerator import BaseViewGenerator, T


class SimpleViewGenerator(BaseViewGenerator[T]):

	def __init__(self, unit: AbsT3Unit, buffers: Iterable[AmpelBuffer], jupdater: StockJournalUpdater) -> None:
		super().__init__(unit_name = unit.__class__.__name__, jupdater = jupdater)
		self.buffers = buffers
		self.View = unit._View

	def __iter__(self) -> Generator[T, JournalAttributes, None]:
		View = self.View
		l = self.stocks
		for ab in self.buffers:
			l.append(ab['id'])
			yield View(**recursive_freeze(ab))
