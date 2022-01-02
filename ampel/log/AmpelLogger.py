#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/log/AmpelLogger.py
# License:             BSD-3-Clause
# Author:              valery brinnel <firstname.lastname@gmail.com>
# Date:                27.09.2018
# Last Modified Date:  18.12.2020
# Last Modified By:    Jakob van Santen <jakob.van.santen@desy.de>

import logging, sys, traceback
from sys import _getframe
from os.path import basename
from typing import Any, TYPE_CHECKING
from ampel.types import ChannelId
from ampel.log.LightLogRecord import LightLogRecord
from ampel.log.LogFlag import LogFlag
from ampel.protocol.LoggingHandlerProtocol import LoggingHandlerProtocol, AggregatingLoggingHandlerProtocol
from ampel.log.handlers.AmpelStreamHandler import AmpelStreamHandler

if TYPE_CHECKING:
	from ampel.mongo.update.var.DBLoggingHandler import DBLoggingHandler

ERROR = LogFlag.ERROR
WARNING = LogFlag.WARNING
SHOUT = LogFlag.SHOUT
INFO = LogFlag.INFO
VERBOSE = LogFlag.VERBOSE
DEBUG = LogFlag.DEBUG

if TYPE_CHECKING:
	from ampel.core.AmpelContext import AmpelContext

class AmpelLogger:

	loggers: dict[int | str, 'AmpelLogger'] = {}
	_counter: int = 0
	verbose: int = 0


	@classmethod
	def get_logger(cls, name: None | int | str = None, force_refresh: bool = False, **kwargs) -> 'AmpelLogger':
		"""
		Creates or returns an instance of :obj:`AmpelLogger <ampel.log.AmpelLogger>`
		that is registered in static dict 'loggers' using the provided name as key.
		If a logger with the given name already exists, the existing logger instance is returned.
		If name is None, unique (int) name will be generated
		:param ``**kwargs``: passed to constructor

		Typical use:\n
		.. sourcecode:: python\n
			logger = AmpelLogger.get_logger()
		"""

		if not name:
			cls._counter += 1
			name = cls._counter

		if name not in AmpelLogger.loggers or force_refresh:
			AmpelLogger.loggers[name] = AmpelLogger(name=name, **kwargs)

		return AmpelLogger.loggers[name]


	@staticmethod
	def from_profile(context: 'AmpelContext', profile: str, run_id: None | int = None, **kwargs) -> 'AmpelLogger':

		handlers = context.config.get(f'logging.{profile}', dict, raise_exc=True)
		logger = AmpelLogger.get_logger(console=False, **kwargs)

		if "db" in handlers:
			# avoid circular import
			from ampel.mongo.update.var.DBLoggingHandler import DBLoggingHandler

			if run_id is None:
				raise ValueError("Parameter 'run_id' is required when log_profile requires db logging handler")

			logger.addHandler(
				DBLoggingHandler(context.db, run_id, **handlers['db'])
			)

		if "console" in handlers:
			logger.addHandler(
				AmpelStreamHandler(**handlers['console'])
			)

		return logger


	@staticmethod
	def get_console_level(context: 'AmpelContext', profile: str) -> None | int:

		handlers = context.config.get(f'logging.{profile}', dict, raise_exc=True)

		if "console" in handlers:
			if 'level' in handlers['console']:
				return handlers['console']['level']
			return LogFlag.INFO.__int__()

		return None


	@classmethod
	def has_verbose_console(cls, context: 'AmpelContext', profile: str) -> bool:

		if lvl := cls.get_console_level(context, profile):
			return lvl < INFO
		return False


	def __init__(self,
		name: int | str = 0,
		base_flag: None | LogFlag = None,
		handlers: None | list[LoggingHandlerProtocol | AggregatingLoggingHandlerProtocol] = None,
		channel: None | ChannelId | list[ChannelId] = None,
		# See AmpelStreamHandler annotations for more details
		console: None | bool | dict[str, Any] = True
	) -> None:

		self.name = name
		self.base_flag = base_flag.__int__() if base_flag else 0
		self.handlers = handlers or []
		self.channel = channel
		self.level = 0
		self.fname = _getframe().f_code.co_filename

		if console:
			self.addHandler(
				AmpelStreamHandler() if console is True else AmpelStreamHandler(**console) # type: ignore
			)
		else:
			self.provenance = False

		self._auto_level()


	def _auto_level(self):

		self.level = min([h.level for h in self.handlers]) if self.handlers else 0
		if self.level < INFO:
			self.verbose = 2 if self.level < VERBOSE else 1
		else:
			if self.verbose != 0:
				self.verbose = 0


	def addHandler(self, handler: LoggingHandlerProtocol) -> None:

		if handler.level < self.level:
			self.level = handler.level

		if isinstance(handler, AmpelStreamHandler) and handler.provenance:
			self.provenance = True

		if self.level < INFO:
			self.verbose = 2 if self.level < VERBOSE else 1

		self.handlers.append(handler)


	def removeHandler(self, handler: LoggingHandlerProtocol) -> None:
		self.handlers.remove(handler)
		self._auto_level()


	def get_db_logging_handler(self) -> 'None | DBLoggingHandler':
		# avoid circular import
		from ampel.mongo.update.var.DBLoggingHandler import DBLoggingHandler
		for el in self.handlers:
			if isinstance(el, DBLoggingHandler):
				return el
		return None


	def break_aggregation(self) -> None:
		for el in self.handlers:
			if isinstance(el, AggregatingLoggingHandlerProtocol):
				el.break_aggregation()


	def error(self, msg: str | dict[str, Any], *args,
		exc_info: None | Exception = None,
		extra: None | dict[str, Any] = None,
	):
		self.log(ERROR, msg, *args, exc_info=exc_info, extra=extra)


	def warn(self, msg: str | dict[str, Any], *args,
		extra: None | dict[str, Any] = None,
	):
		if self.level <= WARNING:
			self.log(WARNING, msg, *args, extra=extra)


	def info(self, msg: None | str | dict[str, Any], *args,
		extra: None | dict[str, Any] = None,
	) -> None:
		if self.level <= INFO:
			self.log(INFO, msg, *args, extra=extra)


	def debug(self, msg: None | str | dict[str, Any], *args,
		extra: None | dict[str, Any] = None,
	):
		if self.level <= DEBUG:
			self.log(DEBUG, msg, *args, extra=extra)


	def handle(self, record: LightLogRecord | logging.LogRecord) -> None:
		for h in self.handlers:
			if record.levelno >= h.level:
				h.handle(record)


	def flush(self) -> None:
		for h in self.handlers:
			h.flush()


	def log(self,
		lvl: int, msg: None | str | dict[str, Any], *args,
		exc_info: None | bool | Exception = None,
		extra: None | dict[str, Any] = None,
	):

		if args and isinstance(msg, str):
			msg = msg % args

		record = LightLogRecord(name=self.name, levelno=lvl | self.base_flag, msg=msg)

		if lvl > WARNING or self.provenance:
			frame = _getframe(1) # logger.log(...) was called directly
			if frame.f_code.co_filename == self.fname:
				frame = _getframe(2) # logger.info(...), logger.debug(...) was used
			record.__dict__['filename'] = basename(frame.f_code.co_filename)
			record.__dict__['lineno'] = frame.f_lineno

		if extra:
			extra = dict(extra)
			if (stock := extra.pop("stock", None)) is not None:
				record.stock = stock
			if (channel := (extra.pop("channel", None) or self.channel)) is not None:
				record.channel = channel
			record.extra = extra

		if exc_info:

			if exc_info == 1:
				exc_info = sys.exc_info() # type: ignore
				lines = traceback.format_exception(*sys.exc_info())
			elif isinstance(exc_info, tuple):
				lines = traceback.format_exception(*sys.exc_info())
			elif isinstance(exc_info, Exception):
				lines = traceback.format_exception(
					type(exc_info), exc_info, exc_info.__traceback__
				)
			else:
				lines = []

			erec = AmpelLogger.fork_rec(record, "\n")
			for h in self.handlers:
				h.handle(erec)

			for el in lines:
				for l in el.split('\n'):
					if not l:
						continue
					erec = AmpelLogger.fork_rec(record, l)
					for h in self.handlers:
						h.handle(erec)

			if record.msg:
				rec2 = AmpelLogger.fork_rec(record, "-" * len(record.msg))
				for h in self.handlers:
					h.handle(record)
					h.handle(rec2)

			return

		for h in self.handlers:
			if record.levelno >= h.level:
				h.handle(record)


	@staticmethod
	def fork_rec(orig: LightLogRecord, msg: str) -> LightLogRecord:
		new_rec = LightLogRecord(name=0, msg=None, levelno=0)
		for k, v in orig.__dict__.items():
			new_rec.__dict__[k] = v
		new_rec.msg = msg
		return new_rec
