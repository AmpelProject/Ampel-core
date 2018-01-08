#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/flags/T2RunStates.py
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 14.12.2017
# Last Modified Date: 07.01.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from enum import Flag

class T2RunStates(Flag):
	"""
	"""

	TO_RUN						= 1
	TO_RUN_PRIO					= 2
	COMPLETED					= 4
	RUNNING						= 8
	ERROR						= 16
