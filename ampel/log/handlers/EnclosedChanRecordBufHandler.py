#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/log/handlers/EnclosedChanRecordBufHandler.py
# License:             BSD-3-Clause
# Author:              valery brinnel <firstname.lastname@gmail.com>
# Date:                01.05.2020
# Last Modified Date:  15.12.2022
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

from logging import Handler, Logger

from ampel.log.handlers.RecordBufferingHandler import RecordBufferingHandler
from ampel.protocol.LoggingHandlerProtocol import LoggingHandlerProtocol
from ampel.types import ChannelId, StockId


class EnclosedChanRecordBufHandler(RecordBufferingHandler):

	__slots__ = '_channel', '_unit', '_empty_msg'


	def __init__(self, level: int, channel: ChannelId, unit: None | str = None) -> None:
		super().__init__(level)
		self._channel = channel
		self._unit = unit
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

				rec.stock = stock

				if extra:
					if 'extra' in rec.__dict__:
						rec.extra = {**rec.extra, **extra} # type: ignore[union-attr,dict-item]
					else:
						rec.extra = extra

				if self._unit:
					rec.unit = self._unit

				target.handle(rec) # type: ignore[arg-type]

		if clear:
			self.buffer.clear()
			self.has_error = False
