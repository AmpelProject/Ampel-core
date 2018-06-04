#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/flags/CompoundFlags.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 20.03.2018
# Last Modified Date: 15.05.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from enum import Flag

class CompoundFlags(Flag):
	"""
	"""
	PARTNERSHIP_DATA    = 1 
	HAS_REPROCESSED_PPS = 2 
	HAS_UPPER_LIMITS 	= 4
	EXCLUDED_PPS   		= 8
