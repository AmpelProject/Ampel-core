#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/logging/handlers/DefaultRecordBufferingHandler.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 05.05.2020
# Last Modified Date: 05.05.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from logging import Handler, Logger
from typing import Union, Optional, Dict
from ampel.log.AmpelLogger import AmpelLogger
from ampel.abstract.AbsLoggingHandler import AbsLoggingHandler
from ampel.log.handlers.RecordBufferingHandler import RecordBufferingHandler
from ampel.type import StockId, ChannelId


class DefaultRecordBufferingHandler(RecordBufferingHandler):
	"""
	MemoryHandler-like class that can grown infinitely and features a convenience method called 'forward'
	"""

	def forward(self,
		target: Union[AmpelLogger, Logger, AbsLoggingHandler, Handler],
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

			if channel:
				rec.channel = self._channel # type: ignore

			if stock:
				rec.stock = stock # type: ignore

			if extra:
				if 'extra' in rec.__dict__:
					rec.extra = {**rec.extra, **extra} # type: ignore
				else:
					rec.extra = extra # type: ignore

			target.handle(rec) # type: ignore

		if clear:
			self.buffer = []
			self.has_error = False
