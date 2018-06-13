#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/flags/CompoundFlags.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 20.03.2018
# Last Modified Date: 13.06.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from enum import Flag

class CompoundFlags(Flag):
	"""
	"""

	HAS_UPPER_LIMITS 			= 1
	HAS_AUTOCOMPLETED_PHOTO		= 2
	HAS_SUPERSEEDED_PPS   		= 4
	HAS_EXCLUDED_PPS   			= 8
	HAS_MANUAL_EXCLUSION   		= 16
	HAS_DATARIGHTS_EXCLUSION  	= 32

	WITH_CUSTOM_POLICES  		= 64

	INST_ZTF					= 128
	SRC_IPAC					= 256
	ZTF_COLLAB_DATA     		= 512
	ZTF_REPROC_PPS 				= 1024
