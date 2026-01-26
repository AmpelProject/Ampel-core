#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/log/handlers/RecordBufferingHandler.py
# License:             BSD-3-Clause
# Author:              valery brinnel <firstname.lastname@gmail.com>
# Date:                25.09.2018
# Last Modified Date:  23.01.2026
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

from contextlib import suppress
from logging import WARNING, LogRecord
from multiprocessing import Manager, current_process
from ampel.log.LightLogRecord import LightLogRecord


# ruff: noqa: SLF001
class RecordBufferingHandler:
	"""
	MemoryHandler-like class that can grown infinitely.
	The standard memory handler provided by logging (BufferingHandler) makes use of a value called 'capacity',
	which once reached, triggers the flush() method when new log records are emitted.
	Since we trust ourselves to do things right (to never let the buffer grow indefinitely),
	we renounce to use such security measure.

	Known subclasses: DefaultRecordBufferingHandler, ChanRecordBufHandler, EnclosedChanRecordBufHandler
	"""

	__slots__ = 'buffer', 'level', 'has_error', 'warn_lvl', 'mp_queue'
	_manager = None


	def __init__(self, level: int) -> None:
		self.buffer: list[LogRecord | LightLogRecord] = []
		self.level = level
		self.has_error = False
		self.warn_lvl = WARNING
		self.mp_queue = None


	def __del__(self):
		with suppress(Exception):
			self.flush()


	def flush(self) -> None:
		if self.mp_queue:
			if current_process().name == "MainProcess":
				while True:
					try:
						mp_name, batch = self.mp_queue.get_nowait()
					except Exception:
						break
					for record in batch:
						if record.extra:
							record.extra |= {"process": mp_name}
						else:
							record.extra = {"process": mp_name}
						self.handle(record)
			elif self.buffer:
				with suppress(Exception):
					self.mp_queue.put(current_process().name, self.buffer)
				self.buffer = []


	def __getstate__(self):

		if current_process().name != "MainProcess":
			raise RuntimeError("RecordBufferingHandler cannot be pickled inside worker processes") 

		# print("Pickling logger in pid", os.getpid())
		if self.__class__._manager is None:
			self.__class__._manager = Manager()

		if self.mp_queue is None:
			self.mp_queue = self.__class__._manager.Queue()
		
		return {
			'level': self.level,
			'has_error': self.has_error,
			'warn_lvl': self.warn_lvl,
			'mp_queue': self.mp_queue
		}


	def __setstate__(self, state): # called by pickle on unpickling
		self.buffer = []
		self.level = state['level']
		self.has_error = state['has_error']
		self.warn_lvl = state['warn_lvl']
		self.mp_queue = state['mp_queue']


	def clear(self) -> None:
		self.buffer = []
		self.has_error = False


	def handle(self, record: LogRecord | LightLogRecord) -> None:
		if record.levelno >= self.level:
			self.buffer.append(record)
			if record.levelno > self.warn_lvl:
				self.has_error = True
