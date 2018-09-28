#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/logging/AmpelLogger.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 27.09.2018
# Last Modified Date: 28.09.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import logging, sys
from ampel.pipeline.logging.ExtraLogFormatter import ExtraLogFormatter

class AmpelLogger(logging.Logger):

	loggers = {}

	@staticmethod
	def get_unique_logger(**kwargs):
		"""
		Returns a new instance of :obj:`AmpelLogger <ampel.pipeline.logging.AmpelLogger>` at each execution.
		This method calls :func:`get_logger <al.AmpelLogger.get_logger>` with a logger *name* 
		generated using the current time (example: *"Ampel-23:58:39.911725"*).
		Please check method :func:`_new_logger <ampel.pipeline.logging.AmpelLogger._new_logger>` 
		for more info regarding the returned logger.

		:param dict ``**kwargs``: passed to :func:`_new_logger <al.AmpelLogger._new_logger>`
		:returns: :func:`AmpelLogger <al.AmpelLogger>` instance

		Typical use:\n
		.. sourcecode:: python\n
			logger = AmpelLogger.get_unique_logger()
		"""

		from datetime import datetime
		return AmpelLogger.get_logger(
			"Ampel-"+str(datetime.utcnow().time()), **kwargs
		)


	@staticmethod
	def get_logger(name="Ampel", **kwargs):
		"""
		Creates or returns an instance of :obj:`AmpelLogger <ampel.pipeline.logging.AmpelLogger>` 
		that is registered in static dict :func:`loggers <ampel.pipeline.logging.AmpelLogger.loggers>` 
		using the provided name.
		If a logger with the given name already exists, the existing logger instance is returned.
		This method calls :func:`_new_logger <ampel.pipeline.logging.AmpelLogger._new_logger>`  
		with the provided logger *name* (default: *Ampel*). 
		Please check :func:`_new_logger <ampel.pipeline.logging.AmpelLogger._new_logger>`
		for more info regarding the returned logger.

		:param str name: logger name
		:param dict ``**kwargs``: passed to :func:`_new_logger <al.AmpelLogger._new_logger>`
		:returns: :obj:`AmpelLogger <ampel.pipeline.logging.AmpelLogger>` instance

		Typical use:\n
		.. sourcecode:: python\n
			logger = AmpelLogger.get_logger()
		"""

		if name not in AmpelLogger.loggers:
			AmpelLogger.loggers[name] = AmpelLogger._new_logger(
				name, **kwargs
			)

		return AmpelLogger.loggers[name]


	@staticmethod
	def _new_logger(name, log_level=logging.DEBUG, datefmt="%Y-%m-%d %H:%M:%S"):
		"""
		Creates an instance of :obj:`AmpelLogger <ampel.pipeline.logging.AmpelLogger>` 
		with the following properties:\n
		- `propagate` is set to False
		- it is associated with a `logging.StreamHandler` instance initialized with sys.stderr
		- the later uses ampel.pipeline.logging.ExtraLogFormatter as formatter

		:param str name: logger name
		:returns: :obj:`AmpelLogger <ampel.pipeline.logging.AmpelLogger>` instance
		"""

		logger = AmpelLogger(name)
		logger.propagate = False
		logger.setLevel(log_level)
		sh = logging.StreamHandler(sys.stderr)
		sh.setLevel(log_level)
		sh.setFormatter(
			# Allows to print values passed in dict 'extra'
			ExtraLogFormatter(datefmt=datefmt)
		)
		logger.addHandler(sh)

		return logger


	def set_console_log_level(self, lvl):
		"""
		Sets log level of StreamHandler instance possibly associated with this logger.
		If no StreamHandler instance exists, nothing is performed.

		:param int lvl: log level (ex: *logging.DEBUG*)
		:returns: None
		"""

		for handler in self.handlers:
			if isinstance(handler, logging.StreamHandler):
				handler.setLevel(lvl)
				return


	def quieten_console(self):
		""" 
		Shortcut for set_console_log_level(logging.WARN)\n
		:returns: None
		"""

		self.set_console_log_level(logging.WARN)

		
	def louden_console(self):
		""" 
		Shortcut for set_console_log_level(logging.DEBUG)\n
		:returns: None
		"""

		self.set_console_log_level(logging.DEBUG)


	def propagate_log(self, level, msg, exc_info=False):
		"""
		Calls set_console_log_level(logging.DEBUG), logs the log message and 
		then sets the StreamHandler log level back to its initial value.
		On production, the StreamHandler log level is usually set to WARN.
		It is indeed unnecessary to produce a lot of console logs since we are 
		saving log entries in the DB anyway (using DBLoggingHandler).
		However, selected INFO log entries might be worth logging on the console.

		:param int level: log level (ex: logging.INFO)
		:param str msg: message to be logged
		:param bool exc_info: whether to log the possibly existing exception stack
		:returns: None
		"""

		for handler in self.handlers:
			if isinstance(handler, logging.StreamHandler):
				previous_level = handler.level
				try:
					handler.level = logging.DEBUG
					self.log(level, msg, exc_info=exc_info)
				finally:
					handler.level = previous_level
				break
				
		if level < logging.WARN:
			level = logging.WARN

		self.log(level, "Forced log propagation: "+msg, exc_info=exc_info)


	def _log(self, level, msg, args, exc_info=None, extra=None, stack_info=False):
		"""
		Parent method override in order to fix the following issue: 
		anything passed to 'extra' is added as a property of LogRecord and formatters/handlers 
		cannot distinguish it from other LogRecord properties. To access those, you would need to use: 
		log_record.property_name but how to know the property names of those can vary ?
		As parade, we nest the 'extra' dict into a another dict: {'extra': <provided_extra>}
		Formatters and Handlers can thus access a single property using getattr(log_record, 'extra', None)
		and loop over dict keys.
		"""

		fn, lno, func, sinfo = self.findCaller(stack_info)

		if exc_info:
			if isinstance(exc_info, BaseException):
				exc_info = (type(exc_info), exc_info, exc_info.__traceback__)
			elif not isinstance(exc_info, tuple):
				exc_info = sys.exc_info()

		record = self.makeRecord(
			self.name, level, fn, lno, msg, args, exc_info, func, 
			extra if extra is None else {'extra': extra}, sinfo
		)

		self.handle(record)
