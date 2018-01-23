#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/flags/PhotoPointPolicy.py
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 13.12.2017
# Last Modified Date: 13.01.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>
from enum import Flag

class PhotoPointPolicy(Flag):
	"""
		Flags used in class PhotoPoint.
		Not synced with DB
	"""

	USE_WEIZMANN_SUB		=	1
	USE_HUMBOLDT_ZP			=	2
