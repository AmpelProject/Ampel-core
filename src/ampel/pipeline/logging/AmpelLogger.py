#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/logging/AmpelLogger.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 27.09.2018
# Last Modified Date: 17.10.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import logging, sys
from logging import _logRecordFactory
from ampel.pipeline.logging.ExtraLogFormatter import ExtraLogFormatter
from ampel.pipeline.config.ReadOnlyDict import ReadOnlyDict

# Custom log int level (needed for efficient storing in DB)
logging.DEBUG = 65536 
logging.VERBOSE = 131072
logging.INFO = 262144
logging.SHOUT = 262145
logging.WARNING = 524288
logging.ERROR = 1048576

logging.addLevelName(logging.DEBUG, "DEBUG")
logging.addLevelName(logging.VERBOSE, "VERBOSE")
logging.addLevelName(logging.INFO, "INFO")
logging.addLevelName(logging.SHOUT, "SHOUT")
logging.addLevelName(logging.WARNING, "WARNING")
logging.addLevelName(logging.ERROR, "ERROR")

class AmpelLogger(logging.Logger):

	loggers = {}
	current_logger = None
	aggregation_ok = True

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

		import time
		return AmpelLogger.get_logger(
			name="Ampel-"+str(time.time()), 
			**kwargs
		)


	@staticmethod
	def get_logger(name="Ampel", force_refresh=False, **kwargs):
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

		if name not in AmpelLogger.loggers or force_refresh:
			AmpelLogger.loggers[name] = AmpelLogger._new_logger(
				name, **kwargs
			)

		return AmpelLogger.loggers[name]


	@staticmethod
	def _new_logger(name, log_level=logging.DEBUG, formatter=None, channels=None, formatter_options={}):
		"""
		Creates an instance of :obj:`AmpelLogger <ampel.pipeline.logging.AmpelLogger>` 
		with the following properties:\n
		- `propagate` is set to False
		- it is associated with an AmpelLoggingStreamHandler instance initialized with sys.stderr
		- the later uses ampel.pipeline.logging.ExtraLogFormatter as formatter

		:param str name: logger name
		:returns: :obj:`AmpelLogger <ampel.pipeline.logging.AmpelLogger>` instance

		:param dict formatter_options: possible keys: 'datefmt' (default "%Y-%m-%d %H:%M:%S"), 
		'line_number' (bool, default false), 'channels' (default: None)
		"""

		logger = AmpelLogger(name, channels=channels)
		logger.propagate = False
		logger.setLevel(log_level)
		
		# Perform import here to avoid cyclic import errro
		from ampel.pipeline.logging.AmpelLoggingStreamHandler import AmpelLoggingStreamHandler
		sh = AmpelLoggingStreamHandler(sys.stderr)
		sh.setLevel(log_level)
		sh.setFormatter(
			# Allows to print values passed in dict 'extra'
			ExtraLogFormatter(**formatter_options) if formatter is None else formatter
		)
		logger.addHandler(sh)

		return logger


	@classmethod
	def quieten_console_loggers(self):
		""" 
		Quieten all loggers registered in AmpelLogger.loggers.
		See quieten_console (without s) docstring for more info
		:returns: None
		"""
		for logger in AmpelLogger.loggers:
			logger.set_console_log_level(21)

		
	@classmethod
	def louden_console_loggers(self):
		""" 
		Louden all loggers registered in AmpelLogger.loggers.
		See louden_console (without s) docstring for more info
		:returns: None
		"""
		for logger in AmpelLogger.loggers:
			logger.set_console_log_level(logging.DEBUG)


	def __init__(self, name, level=logging.DEBUG, channels=None):
		""" 
		:param str name:
		:param int level:
		:param channels: 
		:type channels: list(str), str
		"""

		super().__init__(name, level)
		self.__extra = None

		if channels:
			if type(channels) not in (list, int, str):
				raise ValueError("Unsupported type for parameter 'channels' (%s)" % type(channels))
			self.__extra = ReadOnlyDict({'channels': channels})


	def __add_extra(self, key, value):
		"""
		Note: whatever you add here, it must be BSON encodable
		"""
		if self.__extra:
			d = dict(self.__extra)
			d = value
			self.__extra = ReadOnlyDict(d)
		else:
			self.__extra = ReadOnlyDict({key: value})



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
		Shortcut for set_console_log_level(21)\n
		:returns: None
		"""

		self.set_console_log_level(21)

		
	def louden_console(self):
		""" 
		Shortcut for set_console_log_level(logging.DEBUG)\n
		:returns: None
		"""

		self.set_console_log_level(logging.DEBUG)


	def shout(self, msg, *args, **kwargs):
		"""
		shout: custom msg with log level SHOUT (21) that should make its way \
		through the StreamHandler (even quietened)
		"""
		self._log(21, msg, args, **kwargs)


	def verbose(self, msg, *args, **kwargs):
		"""
		shout: custom msg with log level VERBOSE (15)
		"""
		self._log(15, msg, args, **kwargs)


	def propagate_log(self, level, msg, exc_info=False, extra=None):
		"""
		| Calls set_console_log_level(logging.DEBUG), logs the log message and \
		then sets the StreamHandler log level back to its initial value. 
		| On production, the StreamHandler log level is usually set to WARN \
		because it is unnecessary to emit a lot of console logs since \
		log entries are saved in the DB anyway (using DBLoggingHandler).
		| Selected INFO log entries are worth logging on the console though, hence this method.

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

		self.log(
			level, "Forced log propagation: %s" % msg, 
			exc_info=exc_info, extra=extra
		)


	def makeRecord(self, name, level, fn, lno, msg, args, exc_info, func=None, extra=None, sinfo=None):
		"""
		Override of parent factory method.

		Anything passed to 'extra' during logging is added as a property of LogRecord and formatters/handlers 
		cannot distinguish it from other LogRecord properties. To access those, you would need to use: 
		log_record.property_name but how to know the property names of those can vary ?
		We nest the 'extra' dict into a another dict: {'extra': <provided_extra>}
		Formatters and Handlers can thus access a single property using getattr(log_record, 'extra', None)
		and loop over dict keys.
		"""

		if AmpelLogger.current_logger == self:
			AmpelLogger.aggregation_ok = True
		else:
			if AmpelLogger.current_logger: # not the first logging
				AmpelLogger.aggregation_ok = False
			AmpelLogger.current_logger = self

		rv = _logRecordFactory(name, level, fn, lno, msg, args, exc_info, func, sinfo)

		if extra:
			rv.__dict__['extra'] = {**extra, **self.__extra} if self.__extra else extra
		else:
			rv.__dict__['extra'] = self.__extra
				
		return rv
