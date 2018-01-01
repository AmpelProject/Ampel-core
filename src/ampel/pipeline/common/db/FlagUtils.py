#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/common/db/FlagUtils.py
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 14.12.2017
# Last Modified Date: 01.01.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>
from bson import Binary

class FlagUtils():

	
	@staticmethod
	def enumflag_to_mongoflag(enum_flag):
		
		mongo_flag = []
		for i, el  in enumerate(type(enum_flag), 1):
			if el in enum_flag:
				mongo_flag.append(i)

		return mongo_flag


	@staticmethod
	def mongoflag_to_enumflag(mongo_flag, enum_meta):
		
		enum_flag = enum_meta(0)

		for i, el in enumerate(enum_meta, 1):
			if i in mongo_flag:
				enum_flag |= el

		return enum_flag


	@staticmethod
	def int_to_bindata(int_arg):
		"""
			converts a python integer number (unlimited length) into a MongoDB BSON data type 'BinData'. 
			The used subtype 0 (\\x00): "Generic binary subtype"
			The int to bytes conversion uses the little Endian byte ordering 
			(most significant byte is at the end of the byte array)
		"""
		return Binary(
			int_arg.to_bytes(
				(int_arg.bit_length() + 7) // 8,
				'little'
			),
			0
		)

	@staticmethod
	def bindata_bytes_to_int(bin_data_bytes):
		"""
			converts a BSON data type 'BinData' (subtype 0) into a python integer number
			The little Endian byte ordering is used 
			(most significant byte is at the end of the byte array)
		"""
		return int.from_bytes(bin_data_bytes, byteorder='little')
