#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/logging/RecordsBufferingHandler.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 25.09.2018
# Last Modified Date: 08.11.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from logging import WARNING
from logging.handlers import BufferingHandler

class RecordsBufferingHandler(BufferingHandler):
	"""
	MemoryHandler-like class that can grown infinetely 
	(constructor parameter capacity is ignored) 
	and features convenient methods such as 'forward' and 'copy'
	"""

	def __init__(self, embed_channel=False):
		"""
		:param bool embed_channel: 
		"""
		# Set parent capacity to 0
		super().__init__(0) 
		self.has_error = False
		self.embed = embed_channel


	def flush(self):
		"""
		Flush just means: 'erase exisiting log records'
		"""
		self.buffer = []
		self.has_error = False


	def emit(self, record):
		""" 
		"""
		if record.levelno > WARNING:
			self.has_error = True
		self.buffer.append(record)
		

	def forward(self, logger_or_handler, channels=None, extra=None):
		"""
		:param logger_or_handler: logger or LoggingHandler instance

		Forwards saved log records to provided logger instance.
		The internal record buffer will be cleared.
		"""

		if channels:
			if self.embed:
				for el in self.buffer:
					if el.__dict__["msg"]:
						el.__dict__["msg"] = {
							'txt': el.__dict__["msg"],
							'channels': channels
						}
					else:
						el.__dict__["msg"] = {'channels': channels}
			else:
				if extra:
					extra = extra.copy() # shallow copy
					extra['channels'] = channels
				else:
					extra={'channels': channels}

		if extra:
			for el in self.buffer:
				if el.__dict__["extra"]:
					el.__dict__["extra"].update(extra)
				else:
					el.__dict__["extra"] = extra
				logger_or_handler.handle(el)
		else:
			for el in self.buffer:
				logger_or_handler.handle(el)
		self.buffer = []


	def copy(self, logger, channels, extra):
		"""
		Copy saved log records to provided logger instance.
		The internal record buffer is kept.
		"""
		if channels:
			if extra:
				extra = extra.copy() # shallow copy
				extra['channels'] = channels
			else:
				extra={'channels': channels}

		if extra:
			for el in self.buffer:
				if el.__dict__["extra"]:
					el.__dict__["extra"].update(extra)
				else:
					el.__dict__["extra"] = extra
				logger.handle(el)

		else:
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
