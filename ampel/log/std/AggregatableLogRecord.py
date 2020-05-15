#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/logging/AggregatableLogRecord.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 03.10.2019
# Last Modified Date: 03.10.2019
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from logging import LogRecord

class AggregatableLogRecord(LogRecord):
	"""
	Used mainly to print stack traces
	"""
	def __init__(self, d, msg):
		self.__dict__ = d.__dict__.copy()
		self.msg = msg
		self.args = None
