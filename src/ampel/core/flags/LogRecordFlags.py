#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/core/flags/LogRecordFlags.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 14.12.2017
# Last Modified Date: 18.10.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from ampel.base.flags.AmpelMetaFlags import AmpelMetaFlags

class LogRecordFlags(metaclass=AmpelMetaFlags):
	"""
	| Flag used for each log entry stored in the DB.
	| Value fits in a MongoDB int32.
	| First values are imported from generic AmpelFlags \
	and contain information such as INST_ZTF, SRC_IPAC
	"""

	# Log level
	DEBUG                   = 65536 # 2**16
	VERBOSE                 = 131072
	INFO                    = 262144
	WARNING                 = 524288
	ERROR                   = 1048576

	# Execution layer
	T0                      = 2097152
	T1                      = 4194304
	T2                      = 8388608
	T3                      = 16777216

	# Section
	CORE                    = 33554432 
	UNIT                    = 67108864 
	JOB                     = 134217728
	TASK                    = 268435456

	# Run type
	SCHEDULED_RUN           = 536870912
	MANUAL_RUN              = 1073741824 # 2**30
