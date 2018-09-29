#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/logging/RecordsBufferingHandler.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 25.09.2018
# Last Modified Date: 27.09.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from logging.handlers import BufferingHandler

class RecordsBufferingHandler(BufferingHandler):
	"""
	MemoryHandler-like class that can grown infinetely 
	(constructor parameter capacity is ignored) 
	and features convenient methods such as 'forward' and 'copy'
	"""

	def __init__(self):
		super().__init__(0) # 0 capacity but that does not matter
		self.has_error = False


	def flush(self):
		"""
		Flush just means: 'erase exisiting log records'
		"""
		self.buffer = []
		self.has_error = False


	def emit(self, record):
		""" 
		"""
		if record.levelno > 20: # 20 := logging.INFO:
			self.has_error = True
		self.buffer.append(record)
		

	def forward(self, logger_or_handler):
		"""
		Forwards saved log records to provided logger instance.
		The internal record buffer will be deleted.
		:param logger_or_handler: logger or LoggingHandler instance
		"""
		for el in self.buffer:
			logger_or_handler.handle(el)
		self.buffer = []


	def copy(self, logger):
		"""
		Copy saved log records to provided logger instance.
		The internal record buffer is kept.
		"""
		for el in self.buffer:
			logger.handle(el)


	def shouldFlush(self, record):
		"""
		The 'standard' memory handler provided by logging makes use of a value called 
		'capacity', which once reached, triggers the flush() method when new log records are emitted.
		Since we trust ourselves to do things right (to never let the buffer grow indefinitely), 
		we renounce using a such security measure.
		"""
		return False
