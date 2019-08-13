#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/logging/T0ConsoleFormatter.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 21.10.2018
# Last Modified Date: 21.10.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from logging import Formatter

class T0ConsoleFormatter(Formatter):


	def __init__(self, col_name='log', datefmt="%Y-%m-%d %H:%M:%S", line_number=False):
		""" 
		"""
		super().__init__(datefmt=datefmt)
		self.col_name = col_name
		self.line_number = line_number

	def format(self, record):

		extra = getattr(record, 'extra', None)

		out = [
			self.formatTime(record, datefmt=self.datefmt),
			self.col_name,
			record.filename[:-3], # cut the '.py'
			record.levelname,
		]

		if self.line_number:
			out.insert(-2, str(record.lineno))

		if extra:

			# Show compound ids as hex rather than as Binary
			# Note: we *modify* 'extra' but it does not matter because we should 
			# be the last handler (DBLoggingHandler is inserted in first position)
			if 'compId' in extra:
				extra['compId'] = extra['compId'].hex()

			out.append("[%s]" % ', '.join("%s=%s" % itm for itm in extra.items()))

		if record.msg:
			return "<%s>\n  %s" % (" ".join(out), record.msg)
		else:
			return "<%s>" % " ".join(out)
