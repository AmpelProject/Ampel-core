#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/log/handlers/ChanRecordBufHandler.py
# License:             BSD-3-Clause
# Author:              valery brinnel <firstname.lastname@gmail.com>
# Date:                01.05.2020
# Last Modified Date:  15.12.2022
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

from typing import Any
from ampel.log.handlers.RecordBufferingHandler import RecordBufferingHandler
from ampel.protocol.LoggingHandlerProtocol import LoggingHandlerProtocol
from ampel.types import StockId, ChannelId


class ChanRecordBufHandler(RecordBufferingHandler):
	"""
	Record buferring handler that automatically associates
	every log record with a given channel when forwarding
	them to a target logger/handler.

	Note: the addition is not made by overriding the method handle(...)
	on purpose since it is unnecessary to morph/change all log entries:
	logs created during the rejection of alerts are usually not forwarded
	to a standard logger but rather forwarded to special handling classes
	(that save the information into a binary formatted file for instance)
	"""

	__slots__ = '_channel', '_unit', '_extra'

	def __init__(self,
		level: int, channel: ChannelId,
		extra: None | dict[str, Any] = None,
		unit: None | str = None
	) -> None:
		super().__init__(level)
		self._channel = channel
		self._unit = unit
		self._extra = extra


	def forward(self,
		target: LoggingHandlerProtocol,
		stock: None | StockId = None,
		extra: None | dict = None,
		clear: bool = True
	) -> None:
		"""
		Forwards saved log records to provided logger/handler instance.
		Clears the internal record buffer.
		"""
		for rec in self.buffer:

			if rec.levelno >= target.level:

				rec.channel = self._channel # type: ignore[union-attr]

				if stock:
					rec.stock = stock # type: ignore[union-attr]

				if extra:
					if hasattr(rec, 'extra') and rec.extra: # type: ignore[union-attr]
						rec.extra |= extra # type: ignore[union-attr]
					else:
						rec.extra = extra # type: ignore[union-attr]

				if self._extra:
					if hasattr(rec, 'extra') and rec.extra: # type: ignore[union-attr]
						rec.extra |= self._extra # type: ignore[union-attr]
					else:
						rec.extra = self._extra # type: ignore[union-attr]

				if self._unit:
					setattr(rec, 'unit', self._unit)

				target.handle(rec) # type: ignore

		if clear:
			self.buffer.clear()
			self.has_error = False
