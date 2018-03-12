#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/flags/FlagUtils.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 14.12.2017
# Last Modified Date: 11.03.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from bson import Binary
import enum

class FlagUtils():

	
	@staticmethod
	def has_any(enum_flag, *flags):
		for flag in flags:
			if flag in enum_flag:
				return True
		return False


	@staticmethod
	def get_flag_pos_in_enumflag(enum_flag):
		"""
			https://github.com/AmpelProject/Ampel/wiki/Ampel-Flags
		"""
		for i, el in enumerate(type(enum_flag), 1):
			if el in enum_flag:
				return i

		return None


	@staticmethod
	#def enumflag_positions(enum_flag):
	def enumflag_to_dbflag(enum_flag):
		"""
		https://github.com/AmpelProject/Ampel/wiki/Ampel-Flags
		"""
		db_flag = []
		for i, el  in enumerate(type(enum_flag), 1):
			if el.value != 0 and el in enum_flag:
				db_flag.append(i)

		return db_flag


	@staticmethod
	def dbflag_to_enumflag(db_flag, enum_meta):
		"""
		https://github.com/AmpelProject/Ampel/wiki/Ampel-Flags
		"""
		enum_flag = enum_meta(0)

		for i, el in enumerate(enum_meta, 1):
			if i in db_flag:
				enum_flag |= el

		return enum_flag


	@staticmethod
	def contains_enum_flag(flags):
		"""
		"""

		if isinstance(flags.__class__, enum.Flag.__class__):
			return True

		if type(flags) is list:
			for el in flags:
				if isinstance(el.__class__, enum.Flag.__class__):
					return True

		return False


	@staticmethod
	def enum_flags_to_lists(flags):
		"""
		converts enumflag/list of enumflags into (possible 1time nested) 
		list of string representations of enumflag members
		"""
		# 1 element list
		if type(flags) is list and len(flags) == 1:
			flags = flags[0]

		# enum flag instance
		if isinstance(flags.__class__, enum.Flag.__class__):
			return [el.name for el in type(flags) if el.value != 0 and el in flags]

		# list of flag instances
		elif type(flags) is list:
			ret = []
			for flag in flags:
				l = [el.name for el in type(flag) if el.value != 0 and el in flag]
				ret.append(
					l if len(l) != 1 else l[0]
				)
			return ret

		return None


	@staticmethod
	def list_flags_to_enum_flags(flags, flag_class):

		if FlagUtils.is_nested_list(flags):

			ret = []
			for flag in flags:
				f = flag_class(0)
				if type(flag) is list:
					for el in flag:
						try:
							f |= flag_class[el]
						except KeyError:
							raise ValueError("Unknown flag '%s'" % el)
					ret.append(f)
				else:
					ret.append(flag_class[flag])
		else:

			ret = flag_class(0)

			if type(flags) is str:
				try:
					return flag_class[flags]
				except KeyError:
					raise ValueError("Unknown flag '%s'" % flags)
			for el in flags:
				try:
					ret |= flag_class[el]
				except KeyError:
					raise ValueError("Unknown flag '%s'" % el)

		return ret

		
	@staticmethod
	def is_nested_list(inlist):

		# list characterisation (nested or not)
		for el in inlist:
			if type(el) is list:
				return True

		return False

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
