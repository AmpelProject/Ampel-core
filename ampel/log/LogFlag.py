#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/logging/LogFlag.py
# License:             BSD-3-Clause
# Author:              valery brinnel <firstname.lastname@gmail.com>
# Date:                14.12.2017
# Last Modified Date:  11.03.2020
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

from enum import IntFlag


# flake8: noqa: E221
class LogFlag(IntFlag):
	"""
	Flag used for each log entry stored in the DB.
	Value fits in a MongoDB int32.
	bits 0-5: tier
	bits 5-7: run type
	bits 7-8: location (core system or base units)
	bits 9-14: log level
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

	# Log level
	DEBUG                   = 1<<9
	VERBOSE                 = 1<<10
	INFO                    = 1<<11
	SHOUT                   = 1<<12 # SHOUT is for convenience only, saved as INFO into DB
	WARNING                 = 1<<13
	ERROR                   = 1<<14
