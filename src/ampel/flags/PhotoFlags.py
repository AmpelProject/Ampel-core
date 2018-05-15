#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/flags/PhotoFlags.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 14.12.2017
# Last Modified Date: 15.05.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from enum import Flag

class PhotoFlags(Flag):

	"""
	Flags related to photometric measurements, 
	used in the photopoint and upper limit documents.
	This class can embbed more than 64 different flags.
	"""

	PHOTOPOINT				=	1
	UPPERLIMIT				=	2
	DATE_JD					=	4
	FLUX_MAG_VEGA			=	8
	FLUX_MAG_AB				=	16
	INST_ZTF				=	32
	INST_ASASN				=	64
	INST_SNIFS				=	128
	INST_OTHER1				=	256
	INST_OTHER2				=	512
	INST_OTHER3				=	1024
	BAND_ZTF_G				=	2048
	BAND_ZTF_R				=	4096
	BAND_ZTF_I				=	8192
	ZTF_COLLAB				=	16384
	ZTF_PUBLIC				=	32768
	ZTF_HIGH_CADENCE		=	65536
	ALERT_IPAC				=	131072
	ALERT_NUGENS			=	262144
	SRC_IPAC				=	524288
	SRC_AMPEL				=	1048576
	SRC_NUGENS				=	4194304
	HAS_WEIZMANN_SUB		=	8388608
	HAS_HUMBOLDT_ZP			=	16777216
	SUPERSEEDED				=	33554432
	IMAGE_BAD_CALIBRATION 	=	67108864
	IMAGE_ARTIFACT			=	134217728
	IMAGE_TRACKING_PBLM		=	268435456
