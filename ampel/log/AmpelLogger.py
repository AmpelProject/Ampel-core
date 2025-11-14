#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/log/AmpelLogger.py
# License:             BSD-3-Clause
# Author:              valery brinnel <firstname.lastname@gmail.com>
# Date:                27.09.2018
# Last Modified Date:  11.11.2025
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

import sys, logging, traceback
from os.path import basename
from sys import _getframe
from typing import TYPE_CHECKING, Any
from typing_extensions import Self

from ampel.types import ChannelId
from ampel.abstract.AbsContextManager import AbsContextManager
from ampel.log.handlers.AmpelStreamHandler import AmpelStreamHandler
from ampel.log.LightLogRecord import LightLogRecord
from ampel.log.LogFlag import LogFlag
from ampel.protocol.LoggingHandlerProtocol import AggregatingLoggingHandlerProtocol, LoggingHandlerProtocol
from ampel.util.hash import build_unsafe_dict_id


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

class AmpelLogger(AbsContextManager):

	loggers: dict[int | str, Self] = {}
	_counter: int = 0
	verbose: int = 0


	@classmethod
	def get_logger(cls, force_refresh: bool = False, **kwargs) -> Self:
		"""
		Get or create an :class:`AmpelLogger <ampel.log.AmpelLogger>` instance
		based on the provided keyword arguments.

		The logger is stored in the class-level ``loggers`` registry under a key:
		- If no arguments are provided, the registry key defaults to ``"default"``.
		- If ``name`` is provided in ``kwargs``, it serves as registry key.
		- Otherwise, a unique identifier is derived from the keyword arguments
		  and used as the key.

		Once the key is determined:
		- If a logger with the same key already exists, that instance is returned.
		- If no logger exists for the key, a new one is created and stored.
		- If ``force_refresh`` is ``True``, a new logger is always created and
		  replaces any existing one for the key.

		Important notes:
		- This method is intended primarily for cases where one wants to reuse
		  a logger across calls. It is not suitable if you need to pass
		  instantiated handler objects to the logger. In such cases, call the
		  :class:`AmpelLogger` constructor directly.
		- All values in ``kwargs`` must be serializable. Passing non-serializable
		  objects will break the identifier generation.

		:param force_refresh: If ``True``, always create a new logger instance.
		:param kwargs: Additional keyword arguments passed to the constructor.
		:returns: An :class:`AmpelLogger` instance.

		Example usage::

			# Default logger with "default" key
			logger = AmpelLogger.get_logger()

			# Logger with explicit console level
			logger = AmpelLogger.get_logger(console={'level': DEBUG})

			# Force creation and caching of a new logger
			logger = AmpelLogger.get_logger(force_refresh=True, console={'level': INFO})

			# Logger with explicit name
			logger = AmpelLogger.get_logger(name="custom_logger")
		"""

		if not kwargs:
			kwargs["name"] = "default"

		if "name" not in kwargs:
			kwargs["name"] = build_unsafe_dict_id(kwargs)

		if force_refresh or kwargs["name"] not in cls.loggers:
			cls.loggers[kwargs["name"]] = cls(**kwargs)

		return cls.loggers[kwargs["name"]]


	@classmethod
	def has_logger(cls, name: str) -> bool:
		return name in cls.loggers


	@classmethod
	def from_profile(cls,
		context: 'AmpelContext', profile: str, run_id: int | None = None, **kwargs
	) -> Self:
		"""
		Creates and returns an AmpelLogger instance configured from a named logging profile.

		If no logger exists with the same combination of profile name, run ID, context UUID,
		and additional keyword arguments, a new logger is created and configured.
		Otherwise, a previously created logger with matching parameters
		is returned from the internal cache.

		The logger is configured based on the specified profile from the ampel configuration.
		Depending on the profile, it may include handlers such as DBLoggingHandler (requires run_id)
		and AmpelStreamHandler for console output.

		Parameters:
		- context (AmpelContext): Provides access to configuration and database connections.
		- profile (str): Name of the logging profile to load from config.
		- run_id (int | None): Required if the profile includes a DB logging handler.
		- **kwargs: Additional keyword arguments passed to the logger constructor.

		Returns:
		- AmpelLogger: A logger instance configured according to the specified profile.

		Raises:
		- ValueError: If the profile requires a DBLoggingHandler and no run_id is provided.
		"""

		logger_id = f"{profile}_{run_id}_{build_unsafe_dict_id(kwargs)}_{context.uuid}"
		kwargs["name"] = logger_id

		if logger_id in cls.loggers:
			return cls.get_logger(**kwargs)

		# Note: "console": False ensures DBLoggingHandler is inserted first
		logger = cls.get_logger(**(kwargs | {"console": False}))

		profile_dict = context.config.get(f'logging.{profile}', dict, raise_exc=True)
		if "db" in profile_dict:

			# avoid circular import
			from ampel.mongo.update.var.DBLoggingHandler import DBLoggingHandler # noqa: PLC0415

			if run_id is None:
				raise ValueError("Parameter 'run_id' is required when log_profile requires db logging handler")

			logger.addHandler(
				DBLoggingHandler(context.db, run_id, **profile_dict['db'])
			)

		if "console" in profile_dict:
			logger.addHandler(
				AmpelStreamHandler(**profile_dict['console'])
			)

		return logger


	@staticmethod
	def get_console_level(context: 'AmpelContext', profile: str) -> int | None:

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
		base_flag: LogFlag | None = None,
		handlers: list[LoggingHandlerProtocol | AggregatingLoggingHandlerProtocol] | None = None,
		channel: ChannelId | list[ChannelId] | None = None,
		# See AmpelStreamHandler annotations for more details
		console: bool | dict[str, Any] | None = True
	) -> None:

		self.name = name
		self.base_flag = base_flag.__int__() if base_flag else 0
		self.handlers = handlers or []
		self.channel = channel
		self.level = 0
		self.fname = _getframe().f_code.co_filename

		if console:
			shargs: dict[str, Any] = {"color": False} if (base_flag and LogFlag.UNIT in base_flag) else {}
			if isinstance(console, dict):
				shargs |= console
			self.addHandler(AmpelStreamHandler(**shargs))
		else:
			self.provenance = False

		self._auto_level()


	def __exit__(self, exc_type, exc_value, traceback) -> None:
		self.flush()


	def _auto_level(self) -> None:

		self.level = min([h.level for h in self.handlers]) if self.handlers else 0
		if self.level < INFO:
			self.verbose = 2 if self.level < VERBOSE else 1
		elif self.verbose != 0:
			self.verbose = 0


	def addHandler(self, handler: LoggingHandlerProtocol) -> None:

		self.level = min(handler.level, self.level)

		if isinstance(handler, AmpelStreamHandler) and handler.provenance:
			self.provenance = True

		if self.level < INFO:
			self.verbose = 2 if self.level < VERBOSE else 1

		self.handlers.append(handler)


	def removeHandler(self, handler: LoggingHandlerProtocol) -> None:
		self.handlers.remove(handler)
		self._auto_level()


	def get_db_logging_handler(self) -> 'DBLoggingHandler | None':
		# avoid circular import
		from ampel.mongo.update.var.DBLoggingHandler import (  # noqa: PLC0415
			DBLoggingHandler,
		)
		for el in self.handlers:
			if isinstance(el, DBLoggingHandler):
				return el
		return None


	def break_aggregation(self) -> None:
		for el in self.handlers:
			if isinstance(el, AggregatingLoggingHandlerProtocol | AmpelStreamHandler):
				el.break_aggregation()


	def error(self, msg: str | dict[str, Any], *args,
		exc_info: Exception | None = None,
		stack_info: bool = False,
		stacklevel: int = 1,
		extra: dict[str, Any] | None = None,
	):
		self.log(ERROR, msg, *args, exc_info=exc_info, stack_info=stack_info, stacklevel=stacklevel, extra=extra)


	def warn(self, msg: str | dict[str, Any], *args,
		stack_info: bool = False,
		stacklevel: int = 1,
		extra: dict[str, Any] | None = None,
	):
		if self.level <= WARNING:
			self.log(WARNING, msg, *args, stack_info=stack_info, stacklevel=stacklevel, extra=extra)


	def info(self, msg: str | dict[str, Any] | None, *args,
		stack_info: bool = False,
		stacklevel: int = 1,
		extra: dict[str, Any] | None = None,
	) -> None:
		if self.level <= INFO:
			self.log(INFO, msg, *args, stack_info=stack_info, stacklevel=stacklevel, extra=extra)


	def debug(self, msg: str | dict[str, Any] | None, *args,
		stack_info: bool = False,
		stacklevel: int = 1,
		extra: None | dict[str, Any] = None,
	):
		if self.level <= DEBUG:
			self.log(DEBUG, msg, *args, stack_info=stack_info, stacklevel=stacklevel, extra=extra)


	def handle(self, record: LightLogRecord | logging.LogRecord) -> None:
		for h in self.handlers:
			if record.levelno >= h.level:
				h.handle(record)


	def flush(self) -> None:
		for h in self.handlers:
			h.flush()


	def log(self,
		lvl: int, msg: str | dict[str, Any] | None, *args,
		exc_info: bool | Exception | None = None,
		stack_info: bool = False,
		stacklevel: int = 1,
		extra: dict[str, Any] | None = None,
	):

		if args and isinstance(msg, str):
			msg = msg % args

		record = LightLogRecord(name=self.name, levelno=lvl | self.base_flag, msg=msg)

		if lvl > WARNING or self.provenance:
			frame = _getframe(stacklevel) # logger.log(...) was called directly
			if frame.f_code.co_filename == self.fname:
				frame = _getframe(stacklevel+1) # logger.info(...), logger.debug(...) was used
			record.__dict__['filename'] = basename(frame.f_code.co_filename)
			record.__dict__['lineno'] = frame.f_lineno

		if extra:
			extra = dict(extra)
			if (stock := extra.pop("stock", None)):
				record.stock = stock
			if (channel := (extra.pop("channel", None) or self.channel)):
				record.channel = channel
			if (unit := (extra.pop("unit", None))):
				record.unit = unit
			record.extra = extra

		if exc_info:

			if exc_info == 1:
				exc_info = sys.exc_info() # type: ignore[assignment]
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
