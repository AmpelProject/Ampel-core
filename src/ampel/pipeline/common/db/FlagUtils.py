#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : /Users/hu/Documents/ZTF/Ampel/src/ampel/pipeline/common/db/FlagUtils.py
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 14.12.2017
# Last Modified Date: 14.12.2017
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>
from bson import Binary

class FlagUtils():

	@staticmethod
	def int_to_bindata(intflag):
		"""
			converts a python integer number (unlimited length) into a MongoDB BSON data type 'BinData'. 
			The used subtype 0 (\\x00): "Generic binary subtype"
			The int to bytes conversion uses the little Endian byte ordering 
			(most significant byte is at the end of the byte array)
		"""
		return Binary(
			intflag.to_bytes(
				(intflag.bit_length() + 7) // 8,
				'little'
			),
			0
		)

	@staticmethod
	def bindata_to_int(bytesflag):
		"""
			converts a BSON data type'BinData' (subtype 0) into a python integer number
			The little Endian byte ordering is used 
			(most significant byte is at the end of the byte array)
		"""
		return int.from_bytes(bytesflag, byteorder='little')
