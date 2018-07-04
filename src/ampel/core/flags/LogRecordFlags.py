#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/core/flags/LogRecordFlags.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 14.12.2017
# Last Modified Date: 04.07.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from ampel.base.flags.AmpelMetaFlags import AmpelMetaFlags

class LogRecordFlags(metaclass=AmpelMetaFlags):
	"""
	Flag used for each log entry stored in the DB
	"""
	# log level
	INFO                    = 1048576
	DEBUG                   = 2097152
	WARNING                 = 4194304
	ERROR                   = 8388608
	CRITICAL                = 16777216

	# Convenience flag
	ILB                     = 33554432
	HAS_CANDID              = 67108864
	HAS_CHAN                = 134217728
