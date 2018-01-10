#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/flags/AlertFlags.py
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 14.12.2017
# Last Modified Date: 09.01.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>
from enum import Flag, auto

class AlertFlags(Flag):
	"""
		Flags used by the static AmpelAlert class variable 'flags',
		whose value is not transfered or synced with DB.
		Theses flags allow T0 filters to gain knowledge of the origin of alerts
	"""

	NO_FLAG				= 0

	INST_ZTF			= auto()
	INST_OTHER1			= auto()
	INST_OTHER2			= auto()
	INST_OTHER3			= auto()

	SRC_IPAC			= auto()
	SRC_NUGENS			= auto()
	SRC_OTHER1			= auto()
	SRC_OTHER2			= auto()
