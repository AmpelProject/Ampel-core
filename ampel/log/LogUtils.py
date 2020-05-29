#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/logging/LogUtils.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 30.09.2018
# Last Modified Date: 10.05.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import sys, traceback
from math import log2
from typing import Dict, Optional, Union, List, Any, Literal
from ampel.db.AmpelDB import AmpelDB
from ampel.util.general import has_nested_type
from ampel.log.AmpelLogger import AmpelLogger
from ampel.log.LogRecordFlag import LogRecordFlag


class LogUtils:


	@staticmethod
	# using forward reference for type hinting: "When a type hint contains name that have not
	# been defined yet, that definition may be expressed as string literal, to be resolved later"
	# (PEP 484). This is to avoid cyclic import errors
	def log_exception(
		logger: AmpelLogger, exc: Optional[Exception] = None,
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


	@classmethod
	def report_exception(cls,
		ampel_db: AmpelDB, logger: AmpelLogger, tier: Literal[0, 1, 2, 3],
		exc: Optional[Exception] = None, run_id: Optional[Union[int, List[int]]] = None,
		info: Dict[str, Any] = None
	) -> None:
		"""
		:param tier: Ampel tier level (0, 1, 2, 3)
		:param logger: logger instance (logging module). \
		propagate_log() will be used to print details about the exception
		:param run_id: If provided, the run id associated with the current job \
		will be saved into the reported dict instance.
		:param info: optional dict instance whose values will be included \
		in the document inserted into Ampel_troubles
		"""

		from traceback import format_exc
		from sys import exc_info

		# Don't create report for executions canceled manually
		if exc_info()[0] == KeyboardInterrupt:
			return

		# Feedback
		cls.log_exception(logger, exc)

		# Basis dict
		trouble: Dict[str, Any] = {'tier': tier}

		# Should be provided in most of the cases
		# (run id can alternatively be provided through 'info' though)
		if run_id is not None:
			trouble['run'] = run_id

		# Additional info might have been provided (such as alert information)
		if info is not None:
			trouble.update(info)

		trouble['exception'] = format_exc().replace("\"", "'").split("\n")

		# Populate 'troubles' collection
		LogUtils._insert_trouble(trouble, ampel_db, logger)


	@staticmethod
	def report_error(
		ampel_db: AmpelDB, tier: int, msg: str,
		info: Optional[Dict[str, Any]], logger: AmpelLogger
	) -> None:
		"""
		This method is used to report bad states or errors which are grave enough
		to be worth the creation of a 'trouble document'.
		Information concerning the error can be provided as strine message through the 'msg' argument
		as well as dict through the parameter 'info'.
		This method should not be used to report Exceptions (please use report_exception(...))
		:raises: Should not raise errors
		"""

		# Get filename and line number using inspect
		import inspect
		frame, filename, line_number, function_name, lines, index = inspect.stack()[1]

		trouble = {
			'tier': tier,
			'msg': msg,
			'location': '%s:%s' % (filename, line_number),
		}

		# Additional info might have been provided (such as alert information)
		if info is not None:
			trouble.update(info)

		# Feedback
		logger.error("Error occured", extra=trouble)

		# Populate 'troubles' collection
		LogUtils._insert_trouble(trouble, ampel_db, logger)


	@classmethod
	def get_tier_from_log_flags(cls, flags: Union[int, LogRecordFlag]) -> int:
		for i in (1, 2, 4, 8):
			if i & flags.__int__():
				return int(log2(i))
		return -1


	@staticmethod
	def _insert_trouble(
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


	@classmethod
	def safe_query_dict(cls,
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

		extra = {'match': cls.convert_dollars(match)}

		if update:
			extra['update'] = cls.convert_dollars(update)

		return {dict_key: extra} if dict_key else extra


	@classmethod
	def convert_dollars(cls, arg: Dict[str, Any]) -> Dict[str, Any]:
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
						arg[key.replace("$", "\uFF04")] = arg.pop(key)
					if "." in key:
						arg[key.replace(".", "\u2219")] = arg.pop(key)

			if not has_nested_type(arg, dict): # type: ignore
				return arg

			if not pblm_keys:
				arg = arg.copy()

			for key in arg.keys():
				arg[key] = cls.convert_dollars(arg[key])

		elif isinstance(arg, list):
			if has_nested_type(arg, dict):
				arg = arg.copy()
				return [cls.convert_dollars(el) for el in arg]

		return arg
