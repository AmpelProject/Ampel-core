#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/log/handlers/DefaultRecordBufferingHandler.py
# License:             BSD-3-Clause
# Author:              valery brinnel <firstname.lastname@gmail.com>
# Date:                05.05.2020
# Last Modified Date:  15.12.2022
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

from typing import Any

from ampel.log.handlers.RecordBufferingHandler import RecordBufferingHandler
from ampel.protocol.LoggingHandlerProtocol import LoggingHandlerProtocol
from ampel.types import ChannelId, StockId


class DefaultRecordBufferingHandler(RecordBufferingHandler):
	"""
	MemoryHandler-like class that can grown infinitely and features a convenience method called 'forward'
	"""

	__slots__ = '_extra', '_unit'


	def __init__(self, level: int, extra: None | dict[str, Any] = None, unit: None | str = None) -> None:
		super().__init__(level)
		self._extra = extra or {}
		self._unit = unit


	def forward(self,
		target: LoggingHandlerProtocol,
		channel: None | ChannelId = None,
		stock: None | StockId = None,
		extra: None | dict = None,
		clear: bool = True
	) -> None:
		"""
		Forwards saved log records to provided logger/handler instance.
		Clears the internal record buffer.
		"""
		for rec in self.buffer:

			if rec.levelno < target.level:
				continue

			if channel:
				rec.channel = channel

			if stock:
				rec.stock = stock

			if extra:
				if hasattr(rec, 'extra') and rec.extra:
					rec.extra |= extra
				else:
					rec.extra = extra

			if self._extra:
				if hasattr(rec, 'extra') and rec.extra:
					rec.extra |= self._extra
				else:
					rec.extra = self._extra

			if self._unit:
				rec.unit = self._unit

			target.handle(rec)

		if clear:
			self.buffer.clear()
			self.has_error = False
