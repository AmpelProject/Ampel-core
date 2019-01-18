#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/core/flags/LogRecordFlag.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 14.12.2017
# Last Modified Date: 17.01.2019
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from enum import IntFlag

class LogRecordFlag(IntFlag):
	"""
	| Flag used for each log entry stored in the DB.
	| Value fits in a MongoDB int32.
	"""

	# Log level
	DEBUG                   = 1
	VERBOSE                 = 2
	INFO                    = 4
	WARNING                 = 8
	ERROR                   = 16

	# Execution layer
	T0                      = 32
	T1                      = 64
	T2                      = 128
	T3                      = 256

	# Section
	CORE                    = 512 
	UNIT                    = 1024 
	JOB                     = 2048
	TASK                    = 4096

	# Run type
	SCHEDULED_RUN           = 8192
	MANUAL_RUN              = 16384
