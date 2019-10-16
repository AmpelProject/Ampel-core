#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/logging/LoggingUtils.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 30.09.2018
# Last Modified Date: 11.10.2019
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import sys, traceback, logging
from typing import Dict
from ampel.config.utils.ConfigUtils import ConfigUtils
from ampel.core.flags.LogRecordFlag import LogRecordFlag

class LoggingUtils:


	@staticmethod
	# using forward reference for type hinting: "When a type hint contains name that have not 
	# been defined yet, that definition may be expressed as string literal, tp be resolved later"
	# (PEP 484). This is to avoid cyclic import errors
	def log_exception(logger: "AmpelLogger", exc: Exception=None, extra: Dict=None, last=False, msg=None):
		"""
		:param AmpelLogger logger:
		:param Exception exc:
		:param bool last: whether to print only the last exception in the stack
		:param str msg: Optional message
		"""

		sys_exc = False

		if not exc:

			exc = getattr(sys, "last_value", None)
			sys_exc = True
			logger.propagate_log(
				logging.ERROR, "loading exception from sys", extra=extra
			)

			if not exc:
				logger.propagate_log(
					logging.ERROR, "log_exception(..) was called but not exception could be found",
					extra=extra
				)
				return

		logger.propagate_log(logging.ERROR, "-"*50, extra=extra)

		if msg:
			logger.error(msg)

		if last:
			exc.__context__ = None

		for el in traceback.format_exception(
			etype=type(exc), value=exc, tb=exc.__traceback__
		):
			for ell in el.split('\n'):
				if len(ell) > 0:
					logger.propagate_log(logging.ERROR, ell, extra=extra)

		logger.propagate_log(logging.ERROR, "-"*50, extra=extra)

		# Clear up recorded exception (avoiding potential multiple reports)
		if sys_exc:
			sys.last_value=None
			sys.last_traceback=None
			sys.last_type=None


	@classmethod
	def report_exception(cls, logger, exc, tier, run_id=None, info=None):
		"""
		:param int tier: Ampel tier level (0, 1, 2, 3)
		:param logger: logger instance (logging module). \
		propagate_log() will be used to print details about the exception
		:param int run_id: If provided, the runId associated with the current job \
		will be saved into the reported dict instance.
		:param dict info: optional dict instance whose values will be included \
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
		trouble = {'tier': tier}

		# Should be provided systematically
		if run_id is not None:
			trouble['runId'] = run_id

		# Additional info might have been provided (such as alert information)
		if info is not None:
			trouble.update(info)

		trouble['exception'] = format_exc().replace("\"", "'").split("\n")

		# Populate 'troubles' collection
		LoggingUtils._insert_trouble(trouble, logger)


	@staticmethod
	def report_error(tier, msg, info, logger):
		"""
		:param int tier:
		:param str tier:
		:param dict info:
		:param AmpelLogger logger:
		:returns: None
		:raises: Should not raise errors
		"""

		## Get filename and line number using inspect
		import inspect

		# pylint: disable=unused-variable
		frame,filename,line_number,function_name,lines,index = inspect.stack()[1]

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
		LoggingUtils._insert_trouble(trouble, logger)


	@classmethod
	def get_tier_from_log_flags(cls, flags):
		"""
		:param LogRecordFlag flags:
		"""
		for i, flag in enumerate(LogRecordFlag.T0, LogRecordFlag.T1, LogRecordFlag.T2, LogRecordFlag.T3):
			if flag in flags:
				return i
		return -1


	@staticmethod
	def _insert_trouble(trouble, logger):
		"""
		:raises None:
		"""

		from ampel.db.AmpelDB import AmpelDB

		# Populate troubles collection
		try:
			AmpelDB.get_collection('troubles').insert_one(trouble)

		except Exception:

			# Bad luck (possible cause: DB offline)
			logger.propagate_log(
				logging.ERROR, "Exception occured while populating 'troubles' collection",
				exc_info=True
			)

			logger.propagate_log(
				logging.ERROR, "Unpublished 'troubles' document: %s" % str(trouble),
				exc_info=True
			)


	@classmethod
	def safe_query_dict(cls, match, update=None, dict_key='query'):
		u"""
		| Builds a dict that can be passed as "extra" parameter \
		  to instances of AmpelLogger.
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
		<ampel.logging.LoggingUtils.convert_dollars>`)

		:param dict match:
		:param dict update:
		:returns: dict
		"""

		extra = {'match': cls.convert_dollars(match)}

		if update:
			extra['update'] = cls.convert_dollars(update)

		return {dict_key: extra} if dict_key else extra


	@classmethod
	def convert_dollars(cls, arg):
		"""	
		MongoDB does not allow documents containing dollars in 'top level key' \
		(raises InvalidDocument). In order to log DB queries commands, we substitute \
		the dollar sign with the unicode character 'Fullwidth Dollar Sign': ＄.
		Another option would be do cast the dict to string (what we did before v0.5) \
		but it is less readable and takes more storage space. 
		Nested dict shallow copies are performed.

		:param dict arg:
		:returns: dict
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

			if not ConfigUtils.has_nested_type(arg, dict):
				return arg

			if not pblm_keys:
				arg = arg.copy()

			for key in arg.keys():
				arg[key] = cls.convert_dollars(arg[key])

		elif isinstance(arg, list):
			if ConfigUtils.has_nested_type(arg, dict):
				arg=arg.copy()
				return [cls.convert_dollars(el) for el in arg]

		return arg
