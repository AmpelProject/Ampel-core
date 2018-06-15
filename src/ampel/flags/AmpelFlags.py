#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/flags/AmpelFlags.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 13.06.2018
# Last Modified Date: 13.06.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from enum import IntFlag

class AmpelFlags(IntFlag):
	"""
	General Ampel flags (20)
	"""
	INST_ZTF                 = 1
	SRC_IPAC                 = 2
	SRC_AMPEL                = 4
	RESERVED2                = 8
	RESERVED3                = 16
	RESERVED4                = 32
	RESERVED5                = 64
	RESERVED6                = 126
	RESERVED7                = 256
	RESERVED8                = 512
	RESERVED9                = 1024
	RESERVED10               = 2048
	RESERVED11               = 4096
	RESERVED12               = 8192
	RESERVED13               = 16384
	RESERVED14               = 32768
	RESERVED15               = 65536
	RESERVED16               = 131072
	RESERVED17               = 262144
	RESERVED18               = 524288
