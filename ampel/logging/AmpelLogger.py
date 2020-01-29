#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/logging/AmpelLogger.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 27.09.2018
# Last Modified Date: 22.01.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import logging, sys, traceback
from bson import BSON
from logging import _logRecordFactory # type: ignore
from typing import Dict, TextIO, Optional, Union, Sequence, Any
from ampel.types import ChannelId
from ampel.logging.AggregatableLogRecord import AggregatableLogRecord
from ampel.logging.ExtraLogFormatter import ExtraLogFormatter
from ampel.view.ReadOnlyDict import ReadOnlyDict

# Custom log int level (needed for efficient storing in DB)
logging.DEBUG = 65536 
logging.VERBOSE = 131072 # type: ignore
logging.INFO = 262144
logging.SHOUT = 262145 # type: ignore
logging.WARNING = 524288
logging.WARN = 524288
logging.ERROR = 1048576

logging.addLevelName(logging.DEBUG, "DEBUG")
logging.addLevelName(logging.VERBOSE, "VERBOSE") # type: ignore
logging.addLevelName(logging.INFO, "INFO")
logging.addLevelName(logging.SHOUT, "SHOUT") # type: ignore
logging.addLevelName(logging.WARNING, "WARNING")
logging.addLevelName(logging.WARN, "WARN")
logging.addLevelName(logging.ERROR, "ERROR")


class AmpelLogger(logging.Logger):
	"""
	"""

	loggers: Dict[str, 'AmpelLogger'] = {}
	default_stream = sys.stdout
	current_logger = None
	aggregation_ok = True


	@classmethod
	def set_default_stream(cls, arg: TextIO) -> None:
		"""
		:param arg: either sys.stdout or sys.stderr
		"""
		cls.default_stream = arg


	@staticmethod
	def get_unique_logger(**kwargs) -> 'AmpelLogger':
		"""
		Returns a new instance of :obj:`AmpelLogger <ampel.logging.AmpelLogger>` at each execution.
		This method calls :func:`get_logger <al.AmpelLogger.get_logger>` with a logger *name* 
		generated using the current time (example: *"Ampel-23:58:39.911725"*).
		Please check method :func:`_new_logger <ampel.logging.AmpelLogger._new_logger>` 
		for more info regarding the returned logger.

		:param dict ``**kwargs``: passed to :func:`_new_logger <al.AmpelLogger._new_logger>`

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
	def get_logger(name: str = "Ampel", force_refresh: bool = False, **kwargs) -> 'AmpelLogger':
		"""
		Creates or returns an instance of :obj:`AmpelLogger <ampel.logging.AmpelLogger>` 
		that is registered in static dict :func:`loggers <ampel.logging.AmpelLogger.loggers>` 
		using the provided name.
		If a logger with the given name already exists, the existing logger instance is returned.
		This method calls :func:`_new_logger <ampel.logging.AmpelLogger._new_logger>`  
		with the provided logger *name* (default: *Ampel*). 
		Please check :func:`_new_logger <ampel.logging.AmpelLogger._new_logger>`
		for more info regarding the returned logger.

		:param name: logger name
		:param ``**kwargs``: passed to :func:`_new_logger <al.AmpelLogger._new_logger>`
		:returns: :obj:`AmpelLogger <ampel.logging.AmpelLogger>` instance

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
	def _new_logger(
		name: str, stream: Optional[TextIO] = None,
		log_level: int = logging.DEBUG, formatter: logging.Formatter = None,
		channels: Optional[Union[ChannelId, Sequence[ChannelId]]] = None,
		aggregate_interval: int = 1, formatter_options: Optional[Dict] = None
	) -> 'AmpelLogger':
		"""
		Creates an instance of :obj:`AmpelLogger <ampel.logging.AmpelLogger>` 
		with the following properties:\n
		- `propagate` is set to False
		- it is associated with an AmpelLoggingStreamHandler instance (initialized with provided stream)
		- the later uses ampel.logging.ExtraLogFormatter as formatter

		:param name: logger name
		:param formatter_options: possible keys: 'datefmt' (default "%Y-%m-%d %H:%M:%S"), 
		'line_number' (bool, default false), 'channels' (default: None)
		"""

		logger = AmpelLogger(name, channels=channels)
		logger.propagate = False
		logger.setLevel(log_level)
		
		# Perform import here to avoid cyclic import errro
		from ampel.logging.AmpelLoggingStreamHandler import AmpelLoggingStreamHandler
		sh = AmpelLoggingStreamHandler(
			stream if stream else AmpelLogger.default_stream,
			aggregate_interval=aggregate_interval
		)
		sh.setLevel(log_level)

		if formatter is None:
			formatter = ExtraLogFormatter(**formatter_options) \
				if formatter_options else ExtraLogFormatter()

		# Allows to print values passed in dict 'extra'
		sh.setFormatter(formatter)
		logger.addHandler(sh)

		return logger


	@classmethod
	def quieten_console_loggers(cls) -> None:
		""" 
		Quieten all loggers registered in AmpelLogger.loggers.
		See quieten_console (without s) docstring for more info
		"""
		for logger in cls.loggers.values():
			logger.set_console_log_level(logging.WARNING)

		
	@classmethod
	def louden_console_loggers(cls) -> None:
		""" 
		Louden all loggers registered in AmpelLogger.loggers.
		See louden_console (without s) docstring for more info
		:returns: None
		"""
		for logger in cls.loggers.values():
			logger.set_console_log_level(logging.DEBUG)


	def __init__(self, 
		name: str, level: int = logging.DEBUG, 
		channels: Optional[Union[ChannelId, Sequence[ChannelId]]] = None
	):
		""" """

		super().__init__(name, level)
		self.__extra = None

		if channels:
			if not isinstance(channels, (list, int, str)):
				raise ValueError(
					f"Unsupported type for parameter 'channels' ({type(channels)})"
				)
			self.__extra = ReadOnlyDict({'channels': channels})


	def set_extra(self, key: str, value: Any) -> None:
		"""
		Note: whatever you add here, it must be BSON encodable
		"""
		if self.__extra:
			# pylint: disable=access-member-before-definition
			d = dict(self.__extra)
			if value is None:
				del d[key]
			else:
				d[key] = value
			if len(d) == 0:
				self.__extra = None
			else:
				self.__extra = ReadOnlyDict(d)
		else:
			self.__extra = ReadOnlyDict({key: value})


	def unset_extra(self) -> None:
		"""
		"""
		self.__extra = None


	def set_console_log_level(self, lvl: int) -> None:
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


	def quieten_console(self) -> None:
		""" 
		Shortcut for set_console_log_level(21)\n
		:returns: None
		"""

		self.set_console_log_level(logging.WARNING)

		
	def louden_console(self) -> None:
		""" 
		Shortcut for set_console_log_level(logging.DEBUG)\n
		:returns: None
		"""

		self.set_console_log_level(logging.DEBUG)


	def shout(self, msg: str, *args, **kwargs) -> None:
		"""
		log custom msg with log level SHOUT (21) that should make its way \
		through the StreamHandler (even quietened)
		"""
		self._log(logging.SHOUT, msg, args, **kwargs) # type: ignore


	def verbose(self, msg, *args, **kwargs) -> None:
		"""
		log custom msg with log level VERBOSE (15)
		"""
		self._log(logging.VERBOSE, msg, args, **kwargs) # type: ignore


	def propagate_log(self, 
		level: int, msg: str, extra: Optional[Dict[str, Any]] = None, exc_info: bool = False
	) -> None:
		"""
		| Set StreamHandler logging level to DEBUG, log the log message and \
		then set the StreamHandler log level back to its initial value. 
		| On production, the StreamHandler log level is usually set to WARN \
		because it is unnecessary to emit a lot of console logs since \
		log entries are saved in the DB anyway (using DBLoggingHandler).
		| Selected INFO log entries are worth logging on the console though, hence this method.

		:param level: log level (ex: logging.INFO)
		:param msg: message to be logged
		"""

		for handler in self.handlers:
			if isinstance(handler, logging.StreamHandler):
				previous_level = handler.level
				try:
					handler.setLevel(logging.DEBUG)
					self.log(level, msg, extra=extra, exc_info=exc_info)
				finally:
					handler.setLevel(previous_level)
				break


	def makeRecord(self, 
		name, level, fn, lno, msg, args, exc_info, 
		func=None, extra=None, sinfo=None
	) -> logging.LogRecord:
		"""
		Override of parent factory method.

		Anything passed to 'extra' during logging is added 
		as a property of LogRecord and formatters/handlers 
		cannot distinguish it from other LogRecord properties. 
		To access those, you would need to use: 
		log_record.property_name but how to know the property names of those can vary ?
		We nest the 'extra' dict into a another dict: {'extra': <provided_extra>}
		Formatters and Handlers can thus access a single property 
		using getattr(log_record, 'extra', None) and loop over dict keys.
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

		if exc_info:

			addlog = lambda x: self.handle(AggregatableLogRecord(rv, x))

			if exc_info[0] is None:
				addlog("exc_info was requested but no exception could be found")
				exc_info = None

			if isinstance(exc_info, tuple):
				addlog("-"*50)
				for el in traceback.format_exception(
					etype=type(exc_info[0]), value=exc_info[1], tb=exc_info[2]
				):
					for ell in el.split('\n'):
						if len(ell) > 0:
							addlog(ell)
				addlog("-"*50)

		# Verify serializability at point of call
		# TODO: replace this (performance penalty) by implementing 
		# unit tests for contributed modules
		BSON.encode({'extra': rv.__dict__['extra']})

		return rv
