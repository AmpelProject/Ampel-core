#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/logging/LoggingUtils.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 30.09.2018
# Last Modified Date: 15.10.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from logging import ERROR, CRITICAL, Logger

class LoggingUtils:


	@staticmethod
	def log_exception(logger, exc, last=False, msg=None):
		"""
		:param Logger logger: logger instance (python logging module)
		:param Exception exc: the exception
		:param bool last: whether to print only the last exception in the stack
		:param str msg: Optional message

		logs exception like this:
		2018-09-26 20:37:07 <ipython-input>:11 ERROR ##################################################
		2018-09-26 20:37:07 <ipython-input>:11 ERROR Optional message
		2018-09-26 20:37:07 <ipython-input>:15 ERROR Traceback (most recent call last):
		2018-09-26 20:37:07 <ipython-input>:15 ERROR   File "<ipython-input-28-4b9e4d999eb4>", line 7, in <module>
		2018-09-26 20:37:07 <ipython-input>:15 ERROR     a.add(2)
		2018-09-26 20:37:07 <ipython-input>:15 ERROR AttributeError: 'list' object has no attribute 'add'
		2018-09-26 20:37:07 <ipython-input>:11 ERROR ##################################################

		instead of:
		2018-09-26 20:38:07 <ipython-input>:11 CRITICAL 'list' object has no attribute 'add'
		Traceback (most recent call last):
		  File "<ipython-input-30-548af09552c1>", line 7, in <module>
		    a.add(2)
		AttributeError: 'list' object has no attribute 'add'
		"""

		import traceback
		logger.error("#"*50)

		if msg:
			logger.error(msg)

		if last:
			exc.__context__ = None

		for el in traceback.format_exception(
			etype=type(exc), value=exc, tb=exc.__traceback__
		):
			for ell in el.split('\n'):
				if len(ell) > 0:
					logger.error(ell)

		logger.error("#"*50)


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
		:param Logger logger:
		:returns: None
		:raises: Should not raise errors
		"""

		## Get filename and line number using inspect
		import inspect
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
		logger.propagate_log(ERROR, "Error occured", extra=trouble)

		# Populate 'troubles' collection
		LoggingUtils._insert_trouble(trouble, logger)


	@staticmethod
	def _insert_trouble(trouble, logger):
		"""
		"""

		from ampel.pipeline.db.AmpelDB import AmpelDB

		# Populate troubles collection
		try:
			AmpelDB.get_collection('troubles').insert_one(trouble)
		except:
			# Bad luck (possible cause: DB offline)
			logger.propagate_log(
				ERROR, 
				"Exception occured while populating 'troubles' collection",
				exc_info=True
			)


	@classmethod
	def safe_query_dict(cls, match, update=None):
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
		<ampel.pipeline.logging.LoggingUtils.convert_dollars>`)

		:param dict match:
		:param dict update:
		:returns: dict
		"""
		extra = {'query': {'match': cls.convert_dollars(match)}}

		if update:
			extra['query']['update'] = cls.convert_dollars(update)

		return extra


	@staticmethod
	def convert_dollars(arg):
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

		# shallow copy
		d = arg.copy()

		for key in d.keys():
			if type(d[key]) is dict:
				d[key] = LoggingUtils.convert_dollars(d[key])
			if key.startswith('$'):
				d[key.replace('$', "\uFF04")] = d.pop(key)

		return d
