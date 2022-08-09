#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/t3/stage/BaseViewGenerator.py
# License:             BSD-3-Clause
# Author:              valery brinnel <firstname.lastname@gmail.com>
# Date:                20.04.2021
# Last Modified Date:  05.09.2021
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

from typing import TypeVar
from collections.abc import Generator
from ampel.types import StockId, T3Send
from ampel.view.SnapView import SnapView
from ampel.struct.JournalAttributes import JournalAttributes
from ampel.struct.StockAttributes import StockAttributes
from ampel.mongo.update.MongoStockUpdater import MongoStockUpdater

T = TypeVar("T", bound=SnapView)

class BaseViewGenerator(Generator[T, T3Send, None]):
	""" Meant to be subclassed """

	def __init__(self, unit_name: str, stock_updr: MongoStockUpdater) -> None:
		self.unit_name = unit_name
		self.stock_updr = stock_updr
		self.stocks: list[StockId] = []

	def send(self, jt: T3Send):

		if isinstance(jt, tuple):
			stock_id = jt[0]
			element: JournalAttributes | StockAttributes = jt[1]
		else:
			stock_id, element = self.stocks[-1], jt

		if isinstance(element, StockAttributes):
			tag = element.tag
			name = element.name
			jattrs = element.journal
		else:
			tag, name = None, None
			jattrs = element

		if tag:
			self.stock_updr.add_tag(stock_id, tag)

		if name:
			self.stock_updr.add_name(stock_id, name)

		self.stock_updr.add_journal_record(stock=stock_id, jattrs=jattrs, unit=self.unit_name)

	def throw(self, type=None, value=None, traceback=None):
		raise StopIteration

	def get_stock_ids(self):
		return self.stocks
