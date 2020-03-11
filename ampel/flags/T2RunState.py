#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/flags/T2RunState.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 14.12.2017
# Last Modified Date: 11.03.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from enum import Flag

# flake8: noqa: E221
class T2RunState(Flag):

	TO_RUN               = 0
	TO_RUN_PRIO          = 1

	COMPLETED            = 2
	QUEUED               = 4
	RUNNING              = 8
	EXPORTED             = 16
	ERROR                = 32
	EXCEPTION            = 64

	TOO_MANY_TRIALS      = 128
	# For now, I only see a 'purge' bug being capable of
	# indirectly triggering a UNKNOWN_LINK error
	UNKNOWN_LINK         = 256
	UNKNOWN_CONFIG       = 512
	MISSING_DEPENDENCY   = 1024


