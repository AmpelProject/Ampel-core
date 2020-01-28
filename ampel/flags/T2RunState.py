#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/flags/T2RunState.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 14.12.2017
# Last Modified Date: 18.12.2019
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from enum import Flag

class T2RunState(Flag):

	TO_RUN               = 0
	TO_RUN_PRIO          = 1
	TO_RUN_DEPENDENCY    = 2

	COMPLETED            = 4
	QUEUED               = 8
	RUNNING              = 16
	EXPORTED             = 32

	MISSING_INFO         = 64
	BAD_CONFIG           = 128
	ERROR                = 256
	EXCEPTION            = 512
