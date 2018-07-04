#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/core/flags/CompoundFlags.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 20.03.2018
# Last Modified Date: 04.07.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from ampel.base.flags.AmpelMetaFlags import AmpelMetaFlags

class CompoundFlags(metaclass=AmpelMetaFlags):
	"""
	First 20 powers of two are reserved for general ampel flags
	"""

	HAS_UPPER_LIMITS            = 1048576
	HAS_AUTOCOMPLETED_PHOTO     = 2097152
	HAS_SUPERSEEDED_PPS         = 4194304
	HAS_EXCLUDED_PPS            = 8388608
	HAS_MANUAL_EXCLUSION        = 16777216
	HAS_DATARIGHTS_EXCLUSION    = 33554432

	WITH_CUSTOM_POLICIES        = 67108864
	ZTF_COLLAB_DATA             = 134217728
