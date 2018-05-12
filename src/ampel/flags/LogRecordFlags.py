#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/flags/LogRecordFlags.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 14.12.2017
# Last Modified Date: 12.05.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from enum import Flag, auto

class LogRecordFlags(Flag):
	"""
	Flag used for each log entry stored in the DB
	"""
	# log level
	INFO      			 = auto()
	DEBUG      			 = auto()
	WARNING    			 = auto()
	ERROR    			 = auto()
	CRITICAL   			 = auto()

	# Convenience flag
	ILB	                 = auto()
	HAS_CANDID           = auto()
	HAS_CHAN             = auto()
