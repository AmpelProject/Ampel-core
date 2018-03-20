#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/flags/PhotoPointFlags.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 14.12.2017
# Last Modified Date: 16.03.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>


from enum import Flag

class PhotoPointFlags(Flag):
	"""
		Flags used in each photopoint document.
		This class can embbed more than 64 different flags.
	"""

	DATE_JD					=	1
	FLUX_MAG_VEGA			=	2
	FLUX_MAG_AB				=	4
	INST_ZTF				=	8
	INST_ASSASSIN			=	16
	INST_SNIFS				=	32
	INST_OTHER1				=	64
	INST_OTHER2				=	128
	INST_OTHER3				=	256
	ZTF_G					=	512
	ZTF_R					=	1024
	ZTF_I					=	2048
	ZTF_COLLAB				=	4096
	ZTF_PUBLIC				=	8192
	ZTF_HIGH_CADENCE		=	16384
	ALERT_IPAC				=	32768
	ALERT_NUGENS			=	65536
	SRC_IPAC				=	131072
	SRC_T1					=	262144
	SRC_NUGENS				=	524288
	HAS_WEIZMANN_SUB		=	1048576
	HAS_HUMBOLDT_ZP			=	4194304
	SUPERSEEDED				=	8388608
	IMAGE_BAD_CALIBRATION 	=	16777216
	IMAGE_ARTIFACT			=	33554432
	IMAGE_TRACKING_PBLM		=	67108864
