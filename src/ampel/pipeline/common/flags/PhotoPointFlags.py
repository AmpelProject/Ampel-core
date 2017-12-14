#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : /Users/hu/Documents/ZTF/Ampel/src/ampel/pipeline/common/flags/PhotoPointFlags.py
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 14.12.2017
# Last Modified Date: 14.12.2017
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>
from enum import Flag, auto

class PhotoPointFlags(Flag):
	"""
		Flags used in each photopoint document.
		This class can embbed more than 64 different flags.
		If it does, the $bit update operator will be no longer usable.
		Since Photopoints are rarely updated and the probability of a race condition
		due to concurent updates is rather low, we will have to live with the $set operator.
	"""
	NO_FLAG				 	= 0

	DATE_JD				 	= auto()
	FLUX_MAG_VEGA		 	= auto()
	FLUX_MAG_AB			 	= auto()

	INST_ZTF			 	= auto()
	INST_ASSASSIN		 	= auto()
	INST_SNIFS			 	= auto()
	INST_OTHER1			 	= auto()
	INST_OTHER2			 	= auto()
	INST_OTHER3			 	= auto()

	ZTF_G				 	= auto()
	ZTF_R				 	= auto()
	ZTF_I				 	= auto()
	ZTF_PARTNERSHIP		 	= auto()
	ZTF_HIGH_CADENCE	 	= auto()

	ALERT_IPAC			 	= auto()
	ALERT_NUGENS		 	= auto()

	PP_IPAC				 	= auto()
	PP_NUGENS			 	= auto()
	HAS_WEIZMANN_PHOTO	 	= auto()
	HAS_HUMBOLDT_ZP		 	= auto()

	PP_EXCLUDE				= auto()
	PP_SUPERSEEDED			= auto()
	PP_WEIRD				= auto()

	IMAGE_BAD_CALIBRATION 	= auto()
	IMAGE_ARTIFACT			= auto()
	IMAGE_TRACKING_PBLM		= auto()
