#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/flags/T2RunnableIds.py
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 14.12.2017
# Last Modified Date: 28.01.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from enum import Flag

class T2RunnableIds(Flag):
	"""
		This class can embbed more than 64 different flags.
	"""

	SNCOSMO			= 1
	SNII_LC			= 2
	AGN				= 4
	PHOTO_Z			= 8
	PHOTO_TYPE		= 16
	OTHER1			= 32
	OTHER2			= 64
	OTHER3			= 128
	OTHER4			= 256
	OTHER5			= 512

	def as_list(self):
		return [el for el in T2RunnableIds if el in self]
