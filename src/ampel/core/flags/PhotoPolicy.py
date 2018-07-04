#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : src/ampel/core/flags/PhotoPolicy.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 13.12.2017
# Last Modified Date: 04.07.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from enum import Flag

class PhotoPolicy(Flag):
	"""
	Flags used in class PhotoPoint and UpperLimit.
	Not synced with DB
	"""

	USE_WEIZMANN_SUB          = 1
	USE_HUMBOLDT_ZP           = 2
