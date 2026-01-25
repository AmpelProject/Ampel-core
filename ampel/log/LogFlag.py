#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/log/LogFlag.py
# License:             BSD-3-Clause
# Author:              valery brinnel <firstname.lastname@gmail.com>
# Date:                14.12.2017
# Last Modified Date:  25.01.2026
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

from enum import IntFlag


class LogFlag(IntFlag):
	"""
	Flag used for each log entry stored in the DB.
	Value fits in a MongoDB int32.
	bits 0-5: tier
	bits 5-7: run type
	bits 7-8: location (core system or base units)
	bits 9-10: instance info (live or job system)
	bits 11-16: log level
	"""

	# Execution layer
	T0                      = 1
	T1                      = 2
	T2                      = 4
	T3                      = 8
	T4                      = 16

	# Run type
	SCHEDULED_RUN           = 32
	MANUAL_RUN              = 64

	# Location
	UNIT                    = 128
	CORE                    = 256

	# Ampel instance info
	LIVE                    = 1<<9
	JOB                     = 1<<10

	# Log level
	DEBUG                   = 1<<11
	VERBOSE                 = 1<<12
	INFO                    = 1<<13
	SHOUT                   = 1<<14 # SHOUT is for convenience only, saved as INFO into DB
	WARNING                 = 1<<15
	ERROR                   = 1<<16
