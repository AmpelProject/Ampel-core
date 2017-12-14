#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/common/flags/T2SchedulingFlags.py
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 14.12.2017
# Last Modified Date: 14.12.2017
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from enum import Flag

class T2SchedulingFlags(Flag):
	"""
		This class can embbed more than 64 different flags.
	"""

	T2_SNCOSMO			= 1
	T2_SNII_LC			= 2
	T2_AGN				= 4
	T2_PHOTO_Z			= 8
	T2_PHOTO_TYPE		= 16
	T2_OTHER1			= 32
	T2_OTHER2			= 64
	T2_OTHER3			= 128
	T2_OTHER4			= 256
	T2_OTHER5			= 512
