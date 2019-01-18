#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/core/flags/CompoundFlag.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 20.03.2018
# Last Modified Date: 18.01.2019
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

class CompoundFlag():
	""" """

	# Force subclassing
	def __init__(self):
		raise NotImplementedError()

	HAS_UPPER_LIMITS           = None
	HAS_AUTOCOMPLETED_PHOTO    = None
	HAS_SUPERSEDED_PPS         = None
	HAS_EXCLUDED_PPS           = None
	HAS_MANUAL_EXCLUSION       = None
	HAS_DATARIGHTS_EXCLUSION   = None
	WITH_CUSTOM_POLICIES       = None
	ZTF_COLLAB_DATA            = None
