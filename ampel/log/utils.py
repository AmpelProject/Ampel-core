#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/log/utils.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 30.09.2018
# Last Modified Date: 13.06.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import sys, traceback
from math import log2
from typing import Dict, Optional, Union, Any
from ampel.core.AmpelDB import AmpelDB
from ampel.util.collections import has_nested_type
from ampel.log.AmpelLogger import AmpelLogger
from ampel.log.LogFlag import LogFlag
from ampel.metrics.AmpelMetricsRegistry import AmpelMetricsRegistry
from ampel.protocol.LoggerProtocol import LoggerProtocol

exception_counter = AmpelMetricsRegistry.counter(
	"exceptions",
	"Number of exceptions caught and logged",
)

def log_exception(
	logger: LoggerProtocol, exc: Optional[Exception] = None,
	extra: Optional[Dict] = None, last: bool = False, msg: Optional[str] = None
) -> None:
	"""
	:param last: whether to print only the last exception in the stack
	:param msg: Optional message
	"""

	sys_exc = False

	if not exc:

		exc = getattr(sys, "last_value", None)
		sys_exc = True
		logger.error("loading exception from sys", extra=extra)

		if not exc:
			logger.error(
				"log_exception(..) was called but not exception could be found",
				extra=extra
			)
			return

	# Increment exception counter. Labels for process name and tier are added
	# implicitly in multiprocess mode.
	exception_counter.inc()

	logger.error("-" * 50, extra=extra)

	if msg:
		logger.error(msg)

	if last:
		exc.__context__ = None

	for el in traceback.format_exception(
		etype=type(exc), value=exc, tb=exc.__traceback__
	):
		for ell in el.split('\n'):
			if len(ell) > 0:
				logger.error(ell, extra=extra)

	logger.error("-" * 50, extra=extra)

	# Clear up recorded exception (avoiding potential multiple reports)
	if sys_exc:
		sys.last_value = None
		sys.last_traceback = None
		sys.last_type = None


def report_exception(
	ampel_db: AmpelDB,
	logger: AmpelLogger,
	exc: Optional[Exception] = None,
	process: Optional[str] = None,
	info: Dict[str, Any] = None
) -> None:
	"""
	:param tier: Ampel tier level (0, 1, 2, 3)
	:param logger: logger instance (logging module). \
	propagate_log() will be used to print details about the exception
	:param info: optional dict instance whose values will be included \
	in the document inserted into Ampel_troubles
	"""

	from traceback import format_exc
	from sys import exc_info

	# Don't create report for executions canceled manually
	if exc_info()[0] == KeyboardInterrupt:
		return

	# Feedback
	log_exception(logger, exc, info)

	trouble: Dict[str, Any] = {'tier': get_tier_from_logger(logger)}

	# Logger with db_logging_handler must have a run id
	if db_logging_handler := logger.get_db_logging_handler():
		trouble['run'] = db_logging_handler.run_id

	# Additional info might have been provided (such as alert information)
	if info:
		trouble.update(info)

	if process:
		trouble['process'] = process

	trouble['exception'] = format_exc().replace("\"", "'").split("\n")

	# Populate 'troubles' collection
	insert_trouble(trouble, ampel_db, logger)


def report_error(
	ampel_db: AmpelDB, logger: AmpelLogger,
	msg: Optional[str] = None,
	info: Optional[Dict[str, Any]] = None
) -> None:
	"""
	This method is used to report bad states or errors which are grave enough
	to be worth the creation of a 'trouble document'.
	Information concerning the error can be provided as strine message through the 'msg' argument
	as well as dict through the parameter 'info'.
	This method should not be used to report Exceptions (please use report_exception(...))
	:raises: Should not raise errors
	"""

	# Increment exception counter
	exception_counter.inc()

	# Get filename and line number using inspect
	import inspect
	frame, filename, line_number, function_name, lines, index = inspect.stack()[1]

	trouble: Dict[str, Union[None, int, str]] = {
		'tier': get_tier_from_logger(logger),
		'location': '%s:%s' % (filename, line_number),
	}

	if msg:
		trouble['msg'] = msg

	# Additional info might have been provided (such as alert information)
	if info is not None:
		trouble.update(info)

	# Logger with db_logging_handler must have a run id
	if db_logging_handler := logger.get_db_logging_handler():
		trouble['run'] = db_logging_handler.run_id

	# Feedback
	logger.error("Error occured", extra=trouble)

	# Populate 'troubles' collection
	insert_trouble(trouble, ampel_db, logger)


def get_tier_from_logger(logger: AmpelLogger) -> Optional[int]:

	lb = LogFlag(logger.base_flag)
	if LogFlag.T0 in lb:
		return 0
	elif LogFlag.T1 in lb:
		return 1
	elif LogFlag.T2 in lb:
		return 2
	elif LogFlag.T3 in lb:
		return 3

	return None


def get_tier_from_log_flags(flags: Union[int, LogFlag]) -> int:
	for i in (1, 2, 4, 8):
		if i & flags.__int__():
			return int(log2(i))
	return -1


def insert_trouble(
	trouble: Dict[str, Any], ampel_db: AmpelDB, logger: AmpelLogger
) -> None:

	# Populate troubles collection
	try:
		ampel_db \
			.get_collection('troubles') \
			.insert_one(trouble)

	except Exception as e:

		# Bad luck (possible cause: DB offline)
		logger.error(
			msg = "Exception occured while populating 'troubles' collection",
			exc_info=e
		)

		logger.error(
			msg = f"Unpublished 'troubles' document: {str(trouble)}",
			exc_info=e
		)


def safe_query_dict(
	match: Dict[str, Any],
	update: Optional[Dict[str, Any]] = None,
	dict_key: Optional[str] = 'query'
) -> Dict[str, Any]:
	u"""
	| Builds a dict that can be passed as "extra" parameter to instances of AmpelLogger.
	| Returned dict has the following structure:

	.. sourcecode:: python\n
		{
			"query": {
				"match": dict,
				"update": optional_dict
			}
		}

	Possibly embedded dollar signs in dict keys of parameters \
	"match" and "update" are replaced with the the unicode character \
	'Fullwidth Dollar Sign': ＄ (see docstring of :func:`convert_dollars \
	<ampel.log.LogUtils.convert_dollars>`)
	"""

	extra = {'match': convert_dollars(match)}

	if update:
		extra = {
			'match': convert_dollars(match),
			'update': convert_dollars(update)
		}
	else:
		extra = convert_dollars(match)

	return {dict_key: extra} if dict_key else extra


def convert_dollars(arg: Dict[str, Any]) -> Dict[str, Any]:
	"""
	MongoDB does not allow documents containing dollars in 'top level key' \
	(raises InvalidDocument). In order to log DB queries commands, we substitute \
	the dollar sign with the unicode character 'Fullwidth Dollar Sign': ＄.
	Another option would be do cast the dict to string (what we did before v0.5) \
	but it is less readable and takes more storage space.
	Nested dict shallow copies are performed.
	"""

	if isinstance(arg, dict):

		pblm_keys = [key for key in arg.keys() if "$" in key or "." in key]
		if pblm_keys:
			arg = arg.copy() # shallow copy
			for key in pblm_keys:
				if "$" in key:
					v = arg.pop(key)
					arg[(key := key.replace("$", "\uFF04"))] = v
				if "." in key:
					arg[key.replace(".", "\u2219")] = arg.pop(key)

		if not has_nested_type(arg, dict): # type: ignore
			return arg

		if not pblm_keys:
			arg = arg.copy()

		for key in arg.keys():
			arg[key] = convert_dollars(arg[key])

	elif isinstance(arg, list):
		if has_nested_type(arg, dict):
			arg = arg.copy()
			return [convert_dollars(el) for el in arg]

	return arg
