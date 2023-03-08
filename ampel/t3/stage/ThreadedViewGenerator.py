#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/t3/stage/ThreadedViewGenerator.py
# License:             BSD-3-Clause
# Author:              valery brinnel <firstname.lastname@gmail.com>
# Date:                20.04.2021
# Last Modified Date:  26.11.2021
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

from multiprocessing import JoinableQueue
from typing import Generator
from ampel.abstract.AbsT3ReviewUnit import T, T3Send
from ampel.mongo.update.MongoStockUpdater import MongoStockUpdater
from ampel.t3.stage.BaseViewGenerator import BaseViewGenerator


class ThreadedViewGenerator(BaseViewGenerator[T]):
	"""
	Does not craft views but loads them from internal JoinableQueue and yields them
	"""

	def __init__(self, unit_name: str, queue: "JoinableQueue[T]", stock_updr: MongoStockUpdater) -> None:
		super().__init__(unit_name = unit_name, stock_updr = stock_updr)
		self.queue: "JoinableQueue[T]" = queue
		# ensure this generator's queue is consumed at most once 
		self._it = iter(queue.get, None)


	def __iter__(self) -> Generator[T, T3Send, None]:
		for view in self._it:
			self.stocks.append(view.id)
			try:
				yield view
			finally:
				# mark item as done even if the consumer raises an exception
				self.queue.task_done()
