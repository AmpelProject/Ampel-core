#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/core/flags/CompoundFlag.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 20.03.2018
# Last Modified Date: 18.01.2019
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from abc import ABCMeta

class CompoundFlag(metaclass=ABCMeta):
	"""
	"""

	HAS_UPPER_LIMITS           = ()
	HAS_AUTOCOMPLETED_PHOTO    = ()
	HAS_SUPERSEDED_PPS         = ()
	HAS_EXCLUDED_PPS           = ()
	HAS_MANUAL_EXCLUSION       = ()
	HAS_DATARIGHTS_EXCLUSION   = ()
	WITH_CUSTOM_POLICIES       = ()
	ZTF_COLLAB_DATA            = ()
