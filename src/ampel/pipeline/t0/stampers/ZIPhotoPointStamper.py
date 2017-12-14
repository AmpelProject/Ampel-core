#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : /Users/hu/Documents/ZTF/Ampel/src/ampel/pipeline/t0/stampers/ZIPhotoPointStamper.py
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 14.12.2017
# Last Modified Date: 14.12.2017
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>
from ampel.pipeline.common.flags.PhotoPointFlags import PhotoPointFlags

class ZIPhotoPointStamper:

	def __init__(self):
		self.base_flags = PhotoPointFlags.NO_FLAG

	def append_base_flags(self, flags):
		self.base_flags |= flags

	#def ampelize(...):
	def stamp(self, tran_id, pps_list):

		for pp_dict in pps_list:

			# Append tran_id to each photopoint
			pp_dict['_id'] = pp_dict.pop('candid')

			# Append tran_id to each photopoint
			pp_dict['tranId'] = tran_id

			# Base flags
			ppflags = self.base_flags
	
			# Public / private data
			if pp_dict['programpi'] == 'Kulkarni':
				ppflags |= PhotoPointFlags.ZTF_PARTNERSHIP
	
			# Filter color
			if (pp_dict['fid'] == 1):
				ppflags |= PhotoPointFlags.ZTF_G
			elif (pp_dict['fid'] == 2):
				ppflags |= PhotoPointFlags.ZTF_R
			elif (pp_dict['fid'] == 3):
				ppflags |= PhotoPointFlags.ZTF_I
	
			# Add ampel flags
			pp_dict['alFlags'] = ppflags.value

			# HIGH_CADENCE
