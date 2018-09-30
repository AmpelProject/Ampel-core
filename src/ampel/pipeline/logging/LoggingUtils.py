#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/logging/LoggingUtils.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 30.09.2018
# Last Modified Date: 30.09.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>


class LoggingUtils:


	@staticmethod
	def log_exception(logger, ex, last=False, msg=None):
		"""
		:param Logger logger: logger instance (python logging module)
		:param Exception ex: the exception
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
			ex.__context__ = None

		for el in traceback.format_exception(
			etype=type(ex), value=ex, tb=ex.__traceback__
		):
			for ell in el.split('\n'):
				if len(ell) > 0:
					logger.error(ell)

		logger.error("#"*50)


	@staticmethod
	def report_exception(tier, logger=None, dblh=None, info=None):
		"""
		:param int tier: Ampel tier level (0, 1, 2, 3)
		:param logger: optional logger instance (logging module). If provided, 
		propagate_log() will be used to print details about the exception
		:param DBLoggingHandler dblh: instance of ampel.pipeline.logging.DBLoggingHandler.
		If provided, the logId associated with the current job 
		will be saved into the reported dict instance.
		:param dict info: optional dict instance whose values wil be included 
		in the document inserted into Ampel_troubles
		"""

		from ampel.pipeline.db.AmpelDB import AmpelDB
		from traceback import format_exc
		from logging import CRITICAL
		from sys import exc_info

		# Don't create report for executions canceled manually
		if exc_info()[0] == KeyboardInterrupt:
			return 

		if logger:
			# Feedback
			logger.propagate_log(CRITICAL, "Exception occured", exc_info=True)

		# Basis dict 
		trouble = {'tier': tier}

		# Should be provided systematically
		if dblh is not None:
			trouble['logs'] = dblh.get_run_id()

		# Additional info might have been provided (such as alert information)
		if info is not None:
			trouble.update(info)

		trouble['exception'] = format_exc().replace("\"", "'").split("\n")

		# Populate Ampel_trouble collection
		AmpelDB.get_collection('troubles').insert_one(trouble)
