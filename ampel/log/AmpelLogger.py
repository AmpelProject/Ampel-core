#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/logging/AmpelLogger.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 27.09.2018
# Last Modified Date: 23.04.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import logging, sys, traceback
from sys import _getframe
from os.path import basename
from typing import Dict, Optional, Union, Any, ClassVar, List
from ampel.type import ChannelId, StockId
from ampel.log.LighterLogRecord import LighterLogRecord
from ampel.log.LogRecordFlag import LogRecordFlag
from ampel.base.AmpelUnit import AmpelUnit

ERROR = LogRecordFlag.ERROR
WARNING = LogRecordFlag.WARNING
SHOUT = LogRecordFlag.SHOUT
INFO = LogRecordFlag.INFO
VERBOSE = LogRecordFlag.VERBOSE
DEBUG = LogRecordFlag.DEBUG


class AmpelLogger(AmpelUnit):

	loggers: ClassVar[Dict[Union[int, str], 'AmpelLogger']] = {}
	_counter: ClassVar[int] = 0

	level: int = 0 # type: ignore
	name: Union[int, str] = 0
	channel: Optional[Union[ChannelId, List[ChannelId]]] = None
	console_logging: bool = True

	# See AmpelStreamHandler annotations for more details
	console_options: Optional[Dict[str, Any]] = None


	@classmethod
	def get_unique_logger(cls, **kwargs) -> 'AmpelLogger':
		"""
		:returns: a new instance of :obj:`AmpelLogger <ampel.log.AmpelLogger>` for each call.
		Please check method :func:`_new_logger <ampel.log.AmpelLogger._new_logger>`
		for more info regarding the returned logger.
		:param dict ``**kwargs``: passed to :obj:`AmpelLogger <ampel.log.AmpelLogger>`
		Note: The created logger is not referenced by internal static dict 'loggers',
		meaning once it is not longer used/internally referenced, it will be garbage collected.

		Typical use:\n
		.. sourcecode:: python\n
			logger = AmpelLogger.get_unique_logger()
		"""
		cls._counter += 1
		return AmpelLogger(name=cls._counter, **kwargs)


	@staticmethod
	def get_logger(name: Union[int, str] = 0, force_refresh: bool = False, **kwargs) -> 'AmpelLogger':
		"""
		Creates or returns an instance of :obj:`AmpelLogger <ampel.log.AmpelLogger>`
		that is registered in static dict :func:`loggers <ampel.log.AmpelLogger.loggers>`
		using the provided name.
		If a logger with the given name already exists, the existing logger instance is returned.
		This method calls :func:`_new_logger <ampel.log.AmpelLogger._new_logger>`
		with the provided logger *name* (default: *Ampel*).
		Please check :func:`_new_logger <ampel.log.AmpelLogger._new_logger>`
		for more info regarding the returned logger.

		:param name: logger name
		:param ``**kwargs``: passed to :func:`_new_logger <al.AmpelLogger._new_logger>`
		:returns: :obj:`AmpelLogger <ampel.log.AmpelLogger>` instance

		Typical use:\n
		.. sourcecode:: python\n
			logger = AmpelLogger.get_logger()
		"""

		if name not in AmpelLogger.loggers or force_refresh:
			AmpelLogger.loggers[name] = AmpelLogger(name=name, **kwargs)

		return AmpelLogger.loggers[name]


	def __init__(self, **kwargs):

		AmpelUnit.__init__(self, **kwargs)
		self.fname = _getframe().f_code.co_filename
		self.handlers = []

		if self.console_logging:
			self._add_stream_handler(self.console_options)
		else:
			self.provenance = False


	def _add_stream_handler(self, options: Optional[Dict] = None) -> None:

		from ampel.log.handlers.AmpelStreamHandler import AmpelStreamHandler
		sh = AmpelStreamHandler(**(options or {}))
		if options is None or 'level' not in options:
			sh.level = self.level
		self.handlers.append(sh)
		if sh.provenance:
			self.provenance = True


	def addHandler(self, handler: Any) -> None:
		self.handlers.append(handler)


	def removeHandler(self, handler: Any) -> None:
		self.handlers.remove(handler)


	def error(self, msg: Union[str, Dict[str, Any]], *args,
		exc_info: Optional[Exception] = None,
		channel: Optional[Union[ChannelId, List[ChannelId]]] = None,
		stock: Optional[StockId] = None,
		extra: Optional[Dict[str, Any]] = None,
		**kwargs
	):
		self.log(ERROR, msg, *args, exc_info=exc_info, channel=channel or self.channel, stock=stock, extra=extra)


	def warn(self, msg: Union[str, Dict[str, Any]], *args,
		channel: Optional[Union[ChannelId, List[ChannelId]]] = None,
		stock: Optional[StockId] = None,
		extra: Optional[Dict[str, Any]] = None,
		**kwargs
	):
		self.log(WARNING, msg, *args, channel=channel or self.channel, stock=stock, extra=extra)


	def shout(self, msg: Union[str, Dict[str, Any]], *args,
		channel: Optional[Union[ChannelId, List[ChannelId]]] = None,
		stock: Optional[StockId] = None,
		extra: Optional[Dict[str, Any]] = None,
		**kwargs
	):
		"""
		log custom msg with log level SHOUT (21) that should make its way \
		through the StreamHandler (even quietened)
		"""
		self.log(SHOUT, msg, *args, channel=channel or self.channel, stock=stock, extra=extra)


	def info(self, msg: Optional[Union[str, Dict[str, Any]]], *args,
		channel: Optional[Union[ChannelId, List[ChannelId]]] = None,
		stock: Optional[StockId] = None,
		extra: Optional[Dict[str, Any]] = None,
		**kwargs
	) -> None:
		self.log(INFO, msg, *args, channel=channel or self.channel, stock=stock, extra=extra)


	def verbose(self, msg: Optional[Union[str, Dict[str, Any]]], *args,
		channel: Optional[Union[ChannelId, List[ChannelId]]] = None,
		stock: Optional[StockId] = None,
		extra: Optional[Dict[str, Any]] = None,
		**kwargs
	):
		self.log(VERBOSE, msg, *args, channel=channel or self.channel, stock=stock, extra=extra)


	def debug(self, msg: Optional[Union[str, Dict[str, Any]]], *args,
		channel: Optional[Union[ChannelId, List[ChannelId]]] = None,
		stock: Optional[StockId] = None,
		extra: Optional[Dict[str, Any]] = None,
		**kwargs
	):
		self.log(DEBUG, msg, *args, channel=channel or self.channel, stock=stock, extra=extra)


	def handle(self, record: Union[LighterLogRecord, logging.LogRecord]) -> None:
		for h in self.handlers:
			if record.levelno >= h.level:
				h.handle(record)


	def log(self,
		lvl: int, msg: Optional[Union[str, Dict[str, Any]]], *args,
		exc_info: Optional[Union[bool, Exception]] = None,
		channel: Optional[Union[ChannelId, List[ChannelId]]] = None,
		stock: Optional[StockId] = None,
		extra: Optional[Dict[str, Any]] = None,
		**kwargs
	):

		if args and isinstance(msg, str):
			msg = msg % args

		record = LighterLogRecord(name=self.name, levelno=lvl ^ self.level, msg=msg)

		if lvl > WARNING or self.provenance:
			frame = _getframe(1) # logger.log(...) was called directly
			if frame.f_code.co_filename == self.fname:
				frame = _getframe(2) # logger.info(...), logger.debug(...) was used
			record.__dict__['filename'] = basename(frame.f_code.co_filename)
			record.__dict__['lineno'] = frame.f_lineno

		if extra:
			record.extra = extra

		if stock:
			record.stock = stock

		if channel:
			record.channel = channel

		if exc_info:

			if exc_info == 1:
				exc_info = sys.exc_info() # type: ignore
				lines = traceback.format_exception(*sys.exc_info())
			elif isinstance(exc_info, tuple):
				lines = traceback.format_exception(*sys.exc_info())
			elif isinstance(exc_info, Exception):
				lines = traceback.format_exception(
					etype=type(exc_info), value=exc_info, tb=exc_info.__traceback__
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
	def fork_rec(orig: LighterLogRecord, msg: str) -> LighterLogRecord:
		new_rec = LighterLogRecord(name=0, msg=None, levelno=0)
		for k, v in orig.__dict__.items():
			new_rec.__dict__[k] = v
		new_rec.msg = msg
		return new_rec
