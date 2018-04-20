#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/flags/TransientFlags.py
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 14.12.2017
# Last Modified Date: 20.02.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from enum import Flag

class TransientFlags(Flag):
	"""
	"""

	NO_FLAG						= 0

	INST_ZTF					= 1
	INST_OTHER1					= 2
	INST_OTHER2					= 4
	INST_OTHER3					= 8

	ALERT_IPAC					= 16
	ALERT_NUGENS				= 32
	ALERT_OTHER					= 64

	HAS_IPAC_PPT				= 128
	HAS_WZM_PPT					= 256
	HAS_HU_PPT					= 512
	HAS_SUPERSEEDED_PPT			= 1024

	HAS_ERROR					= 2048

	T1_AUTO_COMPLETE			= 2097152
