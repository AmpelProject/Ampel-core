#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/logging/handlers/ChanRecordBufHandler.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 01.05.2020
# Last Modified Date: 05.05.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from logging import Logger, Handler
from typing import Union, Optional, Dict
from ampel.log.handlers.RecordBufferingHandler import RecordBufferingHandler
from ampel.log.handlers.LoggingHandlerProtocol import LoggingHandlerProtocol
from ampel.type import StockId, ChannelId


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

	__slots__ = '_channel',

	def __init__(self, channel: ChannelId) -> None:
		super().__init__()
		self._channel = channel


	def forward(self,
		target: Union[Logger, Handler, LoggingHandlerProtocol],
		stock: StockId,
		extra: Optional[Dict] = None,
		clear: bool = True
	) -> None:
		"""
		Forwards saved log records to provided logger/handler instance.
		Clears the internal record buffer.
		"""
		for rec in self.buffer:

			rec.channel = self._channel # type: ignore[union-attr]
			rec.stock = stock # type: ignore[union-attr]

			if extra:
				if 'extra' in rec.__dict__:
					rec.extra = {**rec.extra, **extra} # type: ignore
				else:
					rec.extra = extra # type: ignore

			target.handle(rec) # type: ignore

		if clear:
			self.buffer = []
			self.has_error = False
