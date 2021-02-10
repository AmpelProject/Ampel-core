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
	"""States of a :class:`~ampel.content.T2Record.T2Record`"""

	COMPLETED            = 0
	TO_RUN               = -1
	TO_RUN_PRIO          = -2
	TO_RUN_LATER         = -3
	QUEUED               = -4
	RUNNING              = -5
	EXPORTED             = -6
	ERROR                = -7
	EXCEPTION            = -8

	TOO_MANY_TRIALS      = -9

	# UNKNOWN_* may be caused by a bug of the ingester or purge
	UNKNOWN_LINK         = -10
	UNKNOWN_CONFIG       = -11
	MISSING_DEPENDENCY   = -12
	MISSING_INFO         = -13
