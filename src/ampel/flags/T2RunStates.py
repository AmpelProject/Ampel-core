#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/flags/T2RunStates.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 14.12.2017
# Last Modified Date: 13.06.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from enum import Flag

class T2RunStates(Flag):
	"""
	"""

	TO_RUN                      = 1
	TO_RUN_PRIO                 = 2
	TO_RUN_DEPENDENCY           = 4

	COMPLETED                   = 8
	QUEUED                      = 16
	RUNNING                     = 32

	MISSING_INFO                = 64
	BAD_CONFIG                  = 128
	ERROR                       = 256
	EXCEPTION                   = 512
