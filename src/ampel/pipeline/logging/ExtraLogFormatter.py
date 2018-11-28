#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/logging/ExtraLogFormatter.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 01.10.2018
# Last Modified Date: 21.10.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from logging import Formatter

#class AmpelConsoleFormatter(logging.Formatter):
class ExtraLogFormatter(Formatter):


	def __init__(self, datefmt="%Y-%m-%d %H:%M:%S", line_number=False):
		""" 
		"""
		super().__init__(datefmt=datefmt)
		self.line_number = line_number

		self.tohex_ids = ["compId", "cp", "compIdEff", "compIdStrict"]

	def format(self, record):

		extra = getattr(record, 'extra', None)

		out = [
			self.formatTime(record, datefmt=self.datefmt),
			record.filename[:-3], # cut the '.py'
			record.levelname,
		]

		if self.line_number:
			out.insert(-2, str(record.lineno))

		if extra:

			# Show compound ids as hex rather than as Binary
			for key in self.tohex_ids:
				if key in extra:
					extra[key] = extra[key].hex()

			out.append("[%s]" % ', '.join("%s=%s" % itm for itm in extra.items()))

		if record.msg:
			return "<%s>\n  %s" % (" ".join(out), record.msg)
		else:
			return "<%s>" % " ".join(out)
