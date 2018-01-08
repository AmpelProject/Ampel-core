#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/flags/ChannelFlags.py
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 01.01.2018
# Last Modified Date: 03.01.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from enum import Flag

class ChannelFlags(Flag):
	"""
	"""
	NO_CHANNEL					= 1
	CHANNEL_SN					= 2
	CHANNEL_NEUTRINO			= 4
	CHANNEL_RANDOM				= 8
	CHANNEL_LENS				= 16
	CHANNEL_OTHER1				= 32
	CHANNEL_OTHER2				= 64
	CHANNEL_OTHER3				= 128
	CHANNEL_OTHER4				= 256

	def as_list(self):
		return [el for el in ChannelFlags if el in self]
