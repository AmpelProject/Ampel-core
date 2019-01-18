#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel/src/ampel/core/flags/ScienceRecordFlag.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 18.01.2019
# Last Modified Date: 18.01.2019
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

class ScienceRecordFlag():
	""" """	

	# Force subclassing
	def __init__(self):
		raise NotImplementedError()

	HAS_ERROR         = None
	HAS_EXCEPTION     = None
