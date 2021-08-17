#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/t3/stage/BaseViewGenerator.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 20.04.2021
# Last Modified Date: 20.04.2021
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import List, Generator, TypeVar
from ampel.types import StockId
from ampel.view.SnapView import SnapView
from ampel.struct.JournalAttributes import JournalAttributes
from ampel.core.StockJournalUpdater import StockJournalUpdater

T = TypeVar("T", bound=SnapView)

class BaseViewGenerator(Generator[T, JournalAttributes, None]):
	""" Meant to be subclassed """

	def __init__(self, unit_name: str, jupdater: StockJournalUpdater) -> None:
		self.unit_name = unit_name
		self.jupdater = jupdater
		self.stocks: List[StockId] = []

	def send(self, jt: JournalAttributes):
		self.jupdater.add_record(stock=self.stocks[-1], jattrs=jt, unit=self.unit_name)

	def throw(self, type=None, value=None, traceback=None):
		raise StopIteration

	def get_stock_ids(self):
		return self.stocks
