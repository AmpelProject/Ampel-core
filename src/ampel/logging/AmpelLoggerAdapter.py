#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/logging/AmpelLoggerAdapter.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : Unspecified
# Last Modified Date: 30.09.2019
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import logging


class AmpelLoggerAdapter(logging.LoggerAdapter):
	"""
	| Standard logging.LoggerAdapter does not preserve context information.
	| See: https://docs.python.org/3.6/howto/logging-cookbook.html#context-info

	In short, if you declare your LoggerAdapter as such\n\n
	.. sourcecode:: python\n
		log_adapter = LoggerAdapter(
			AmpelLoger.get_logger(), 
			extra={'info': 1}
		)\n

	then log something with additional info\n\n
	.. sourcecode:: python\n
		log_adapter.info(
			"my log msg", 
			extra={'another_info': 2}
		)\n

	*extra* will be nonetheless:
		``{'info': 1}``

	This class merges the two info together and thus provides: 
		``{'info': 1, 'another_info': 2}``
	to the underlying logging handlers.

	.. warning::
		Following extra keywords will be ignored if provided: 
		_id, tier, runId, lvl, msg, filename, lineno, funcName
	"""


	def __init__(self, logger, extra):
		"""
		:param Logger logger: logger instance (python module 'logging')
		:param dict extra: dict intance whose items will be logged
		"""
		super().__init__(logger, extra)
		self.std_extra = {'extra': extra}
	  

	def process(self, msg, kwargs):
		"""
		We override the parent process method to solve issues 1 and 2 
		mentioned in the doctstring of this class

		:returns: None
		"""

		# Note (changeable if need be): 
		# we drop anything that is not 'extra' in the kwargs
		if 'extra' in kwargs:
			# Note: context 'extra' can overwrite LoggerAdapter's default 'extra'
			return msg, {'extra': {**self.extra, **kwargs["extra"]}}
		else:
			return msg, self.std_extra
