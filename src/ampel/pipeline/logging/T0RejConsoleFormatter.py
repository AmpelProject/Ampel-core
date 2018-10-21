#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/logging/T0RejConsoleFormatter.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 21.10.2018
# Last Modified Date: 21.10.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from logging import Formatter

class T0RejConsoleFormatter(Formatter):


	def __init__(self, col_name='rej', datefmt="%Y-%m-%d %H:%M:%S", line_number=False, implicit_channels=None):
		""" 
		"""
		super().__init__(datefmt=datefmt)
		self.implicit_channels = implicit_channels
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

			# See ampel.pipline.t0.Channel docstring for more info
			if self.implicit_channels and 'channels' not in extra:
				out.append(
					"[channels=%s, %s]" % (
						self.implicit_channels, 
						', '.join("%s=%s" % itm for itm in extra.items())
					)
				)
			else:
				out.append("[%s]" % ', '.join("%s=%s" % itm for itm in extra.items()))

		if record.msg:
			return "<%s>\n%s" % (" ".join(out), record.msg)
		else:
			return "<%s>" % " ".join(out)
