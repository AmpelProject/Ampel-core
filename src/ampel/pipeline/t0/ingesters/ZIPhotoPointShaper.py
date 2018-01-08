#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/t0/stampers/ZIPhotoPointStamper.py
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 14.12.2017
# Last Modified Date: 07.01.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>
from ampel.flags.PhotoPointFlags import PhotoPointFlags
from ampel.flags.AlDocTypes import AlDocTypes
from ampel.flags.FlagUtils import FlagUtils

class ZIPhotoPointShaper:


	def __init__(self):
		self.base_flags = PhotoPointFlags.INST_ZTF|PhotoPointFlags.SRC_IPAC


	def append_base_flags(self, flags):
		self.base_flags |= flags


	def ampelize(self, tran_id, pps_list):

		for pp_dict in pps_list:

			# Append tran_id to each photopoint
			pp_dict['_id'] = pp_dict.pop('candid')

			# Append tran_id to each photopoint
			pp_dict['tranId'] = tran_id

			# Set alDocType to PHOTOPOINT
			pp_dict['alDocType'] = AlDocTypes.PHOTOPOINT

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
			pp_dict['alFlags'] = FlagUtils.enumflag_to_dbflag(ppflags)

			# HIGH_CADENCE
