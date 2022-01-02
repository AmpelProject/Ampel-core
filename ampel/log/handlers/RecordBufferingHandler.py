#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/logging/handlers/RecordBufferingHandler.py
# License:             BSD-3-Clause
# Author:              valery brinnel <firstname.lastname@gmail.com>
# Date:                25.09.2018
# Last Modified Date:  09.05.2020
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

from typing import Union
from logging import LogRecord, WARNING
from ampel.log.LightLogRecord import LightLogRecord


class RecordBufferingHandler:
	"""
	MemoryHandler-like class that can grown infinitely.
	The standard memory handler provided by logging (BufferingHandler) makes use of a value called 'capacity',
	which once reached, triggers the flush() method when new log records are emitted.
	Since we trust ourselves to do things right (to never let the buffer grow indefinitely),
	we renounce to use such security measure.

	Known subclasses: DefaultRecordBufferingHandler, ChanRecordBufHandler, EnclosedChanRecordBufHandler
	"""

	__slots__ = 'buffer', 'level', 'has_error', 'warn_lvl'

	def __init__(self, level: int) -> None:
		self.buffer: list[LogRecord | LightLogRecord] = []
		self.level = level
		self.has_error = False
		self.warn_lvl = WARNING


	def flush(self) -> None:
		""" Flush just means erase existing log records """
		self.buffer = []
		self.has_error = False


	def handle(self, record: LogRecord | LightLogRecord) -> None:
		if record.levelno >= self.level:
			self.buffer.append(record)
			if record.levelno > self.warn_lvl:
				self.has_error = True
