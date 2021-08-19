#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/logging/handlers/ChanRecordBufHandler.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 01.05.2020
# Last Modified Date: 05.05.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Union, Optional, Dict, Any
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

	__slots__ = '_channel', '_extra'

	def __init__(self, level: int, channel: ChannelId, extra: Optional[Dict[str, Any]] = None) -> None:
		super().__init__(level)
		self._channel = channel
		self._extra = extra


	def forward(self,
		target: Union[LoggingHandlerProtocol],
		stock: Optional[StockId] = None,
		extra: Optional[Dict] = None,
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

				target.handle(rec) # type: ignore

		if clear:
			self.buffer.clear()
			self.has_error = False
