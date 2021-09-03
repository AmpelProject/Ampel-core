#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/t3/stage/BaseViewGenerator.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 20.04.2021
# Last Modified Date: 20.04.2021
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from os import name
from typing import List, Generator, TypeVar, Union
from ampel.types import StockId
from ampel.view.SnapView import SnapView
from ampel.abstract.T3Send import T3Send
from ampel.struct.JournalAttributes import JournalAttributes
from ampel.struct.StockAttributes import StockAttributes
from ampel.mongo.update.MongoStockUpdater import MongoStockUpdater

T = TypeVar("T", bound=SnapView)

class BaseViewGenerator(Generator[T, T3Send, None]):
	""" Meant to be subclassed """

	def __init__(self, unit_name: str, stock_updr: MongoStockUpdater) -> None:
		self.unit_name = unit_name
		self.stock_updr = stock_updr
		self.stocks: List[StockId] = []

	def send(self, jt: T3Send):
		if isinstance(jt, tuple):
			stock_id = jt[0]
			element: Union[JournalAttributes, StockAttributes] = jt[1]
		else:
			stock_id, element = self.stocks[-1], jt
		if isinstance(element, StockAttributes):
			tag = element.tag
			name = element.name
			jattrs = element.journal
		else:
			tag, name = None, None
			jattrs = element
		self.stock_updr.add_journal_record(stock=stock_id, jattrs=jattrs, tag=tag, name=name, unit=self.unit_name)

	def throw(self, type=None, value=None, traceback=None):
		raise StopIteration

	def get_stock_ids(self):
		return self.stocks
