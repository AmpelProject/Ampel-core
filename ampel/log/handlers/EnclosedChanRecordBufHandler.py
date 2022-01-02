#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/logging/handlers/EnclosedChanRecordBufHandler.py
# License:             BSD-3-Clause
# Author:              valery brinnel <firstname.lastname@gmail.com>
# Date:                01.05.2020
# Last Modified Date:  09.05.2020
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

from logging import Logger, Handler
from ampel.log.handlers.RecordBufferingHandler import RecordBufferingHandler
from ampel.protocol.LoggingHandlerProtocol import LoggingHandlerProtocol
from ampel.types import StockId, ChannelId


class EnclosedChanRecordBufHandler(RecordBufferingHandler):

	__slots__ = '_channel', '_empty_msg'


	def __init__(self, level: int, channel: ChannelId) -> None:
		super().__init__(level)
		self._channel = channel
		self._empty_msg = {'c': channel}


	def forward(self,
		target: Logger | Handler | LoggingHandlerProtocol,
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

				if rec.msg:
					rec.msg = {'c': self._channel, 't': rec.msg}
				else:
					rec.msg = self._empty_msg

				rec.stock = stock # type: ignore

				if extra:
					if 'extra' in rec.__dict__:
						rec.extra = {**rec.extra, **extra} # type: ignore
					else:
						rec.extra = extra # type: ignore

				target.handle(rec) # type: ignore

		if clear:
			self.buffer.clear()
			self.has_error = False
