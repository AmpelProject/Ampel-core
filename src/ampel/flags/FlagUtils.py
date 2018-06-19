#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/flags/FlagUtils.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 14.12.2017
# Last Modified Date: 18.06.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from ampel.pipeline.common.AmpelUtils import AmpelUtils
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
			if el in enum_flag:
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
		if flags is None:
			return None

		# 1 element list
		if type(flags) is list and len(flags) == 1:
			flags = flags[0]

		# enum flag instance
		if isinstance(flags.__class__, enum.Flag.__class__):
			return [[el.name for el in type(flags) if el.value != 0 and el in flags]]

		# list of flag instances 
		# example: [T2UnitIds.SNCOSMO, T2UnitIds.AGN]
		elif type(flags) is list:

			# outer list of 2d list to be returned
			ret = []

			# Loop through enum flag list elements
			for flag in flags:

				# Build list with element
				# example: T2UnitIds.SNCOSMO -> "SNCOSMO"
				# or: T2UnitIds.SNCOSMO|T2UnitIds.PHOTO_Z -> ["SNCOSMO", "PHOTO_Z"]
				l = [el.name for el in type(flag) if el.value != 0 and el in flag]

				# Append inner list 'l' to outer list 'ret'
				ret.append(l[0] if len(l) == 1 else l)

			return ret

		return None


	@staticmethod
	def _1d_list_flags_to_enum_flags(flags, flag_class, combine=False):
		"""
		"""
		if FlagUtils.is_nested_list(flags):
			raise ValueError("Provided list cannot be nested")

		try:
			if combine:
				return flag_class(sum([flag_class[el].value for el in AmpelUtils.iter(flags)]))
			else:
				return [flag_class[el] for el in AmpelUtils.iter(flags)]
		except KeyError:
			raise ValueError("Unknown flag in '%s'" % flags)


	@staticmethod
	def list_flags_to_enum_flags(flags, flag_class):
		"""
		flags: 1d or 2d list of string flags

		simple list -> OR connected flags
		2d list with only 1 member in outer list -> AND connected flags
		2d list with only many members 
			-> outer list is OR connected 
			-> inner lists are AND connected 

		Examples:

		a) "SNCOSMO" -> T2UnitIds.SNCOSMO
		b) ["SNCOSMO"] -> T2UnitIds.SNCOSMO
		c) ["SNCOSMO", "AGN"] -> [T2UnitIds.SNCOSMO, T2UnitIds.AGN] (OR connected)
		d) [["SNCOSMO", "AGN"]] -> T2UnitIds.SNCOSMO|T2UnitIds.AGN (one enum flag instance)
		e) [["SNCOSMO", "AGN"], "PHOTO_Z"] -> [T2UnitIds.SNCOSMO|T2UnitIds.AGN, PHOTO_Z]
		"""

		if flags is None:
			return None

		# Examples d) and e)
		if FlagUtils.is_nested_list(flags):
			ret = [FlagUtils._1d_list_flags_to_enum_flags(el, flag_class, True) for el in AmpelUtils.iter(flags)]
			return ret[0] if len(ret) == 1 else ret

		# Examples a) b) and c)
		else:

			# Example a)
			if type(flags) is str:
				return FlagUtils._1d_list_flags_to_enum_flags([flags], flag_class)[0]

			# Example b) and c)
			for el in flags:
				return FlagUtils._1d_list_flags_to_enum_flags(flags, flag_class)


	@staticmethod
	def list_flags_to_db_flags(flags, flag_class):
		""" """
		if flags is None:
			return None

		return FlagUtils.enumflag_to_dbflag(
			FlagUtils.list_flags_to_enum_flags(flags, flag_class)
		)

		
	@staticmethod
	def is_nested_list(inlist):
		""" """
		# list characterisation (nested or not)
		for el in inlist:
			if type(el) in (list, tuple):
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
