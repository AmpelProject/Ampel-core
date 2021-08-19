#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/logging/handlers/DefaultRecordBufferingHandler.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 05.05.2020
# Last Modified Date: 05.05.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Optional, Dict, Any
from ampel.log.handlers.RecordBufferingHandler import RecordBufferingHandler
from ampel.protocol.LoggingHandlerProtocol import LoggingHandlerProtocol
from ampel.types import StockId, ChannelId


class DefaultRecordBufferingHandler(RecordBufferingHandler):
	"""
	MemoryHandler-like class that can grown infinitely and features a convenience method called 'forward'
	"""

	__slots__ = '_extra',


	def __init__(self, level: int, extra: Optional[Dict[str, Any]] = None) -> None:
		super().__init__(level)
		self._extra = extra or {}


	def forward(self,
		target: LoggingHandlerProtocol,
		channel: Optional[ChannelId] = None,
		stock: Optional[StockId] = None,
		extra: Optional[Dict] = None,
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
				rec.channel = channel # type: ignore[union-attr]

			if stock:
				rec.stock = stock # type: ignore[union-attr]

			if extra:
				if hasattr(rec, 'extra') and rec.extra: # type: ignore[union-attr]
					rec.extra |= extra # type: ignore[union-attr]
				else:
					rec.extra = extra # type: ignore

			if self._extra:
				if hasattr(rec, 'extra') and rec.extra: # type: ignore[union-attr]
					rec.extra |= self._extra # type: ignore[union-attr]
				else:
					rec.extra = self._extra # type: ignore[union-attr]

			target.handle(rec) # type: ignore

		if clear:
			self.buffer.clear()
			self.has_error = False
