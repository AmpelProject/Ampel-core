#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/t2/T2RunState.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 14.12.2017
# Last Modified Date: 08.06.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from enum import IntEnum

# flake8: noqa: E221
class T2RunState(IntEnum):

	COMPLETED            = 0
	TO_RUN               = -1
	TO_RUN_PRIO          = -2
	QUEUED               = -3
	RUNNING              = -4
	EXPORTED             = -5
	ERROR                = -6
	EXCEPTION            = -7

	TOO_MANY_TRIALS      = -8

	# UNKNOWN_* may be caused by a bug of the ingester or purge
	UNKNOWN_LINK         = -9
	UNKNOWN_CONFIG       = -10
	MISSING_DEPENDENCY   = -11
	MISSING_INFO         = -12
