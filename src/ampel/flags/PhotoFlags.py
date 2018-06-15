#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/flags/PhotoFlags.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 14.12.2017
# Last Modified Date: 13.06.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from ampel.flags.AmpelMetaFlags import AmpelMetaFlags

class PhotoFlags(metaclass=AmpelMetaFlags):

	"""
	First 20 powers of two are reserved for general ampel flags
	Flags related to photometric measurements, 
	used in the photopoint and upper limit documents.
	This class can contain more than 64 different flags.
	"""

	PHOTOPOINT                = 1048576
	UPPERLIMIT                = 2097152
	BAND_ZTF_G                = 4194304
	BAND_ZTF_R                = 8388608
	BAND_ZTF_I                = 16777216
	ZTF_COLLAB                = 33554432
	ZTF_PUBLIC                = 67108864
	ZTF_HIGH_CADENCE          = 134217728

	SUPERSEEDED               = 268435456
	HAS_HUMBOLDT_ZP           = 536870912

	IMAGE_BAD_CALIBRATION     = 1073741824
	IMAGE_ARTIFACT            = 2147483648
	IMAGE_TRACKING_PBLM       = 4294967296
	IMAGE_FOCUS_PBLM          = 8589934592
