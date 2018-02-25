#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/flags/FlagUtils.py
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 14.12.2017
# Last Modified Date: 05.01.2018
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
	def enumflag_to_dbquery(flags, field_name):
		"""
		'flags': must be one of ampel.flags.* enum flag instance 
		or a list of instances (same class of course).
		'field_name': name (string) of the database field for the search lookup 
		(required for the $or operator when a list of flag instances is provided)

		General example. Let's consider ampel.flags.ChannelFlags.
		The parameter 'flags' could be:
		-> either an instance of ampel.flags.ChannelFlags 
		  (can contain multiples flags that are 'AND' connected)
		  Search criteria would be that result documents must have all the channels 
		  defined in the channelFlag instance
		-> or list of instances of ampel.flags.ChannelFlags whereby the instances 
		  (the list elements) are connected by the 'OR' logical operator between each other

		Concrete example: 
		-> channel_flags = ChannelFlags.HU_EARLY_SN: 
		   finds transient associated with the channel HU_EARLY_SN

		-> channel_flags = ChannelFlags.HU_EARLY_SN|ChannelFlags.HU_RANDOM
		   finds transient that associated with *both* the channels HU_EARLY_SN and HU_RANDOM

		-> channel_flags = [ChannelFlags.LENS, ChannelFlags.HU_EARLY_SN|ChannelFlags.HU_RANDOM]
		   finds transient that associated with 
		   * either with the LENS channel
		   * or with both the channels HU_EARLY_SN and HU_RANDOM
		"""
		if flags is None:
			raise ValueError('Illegal parameters')

		if isinstance(flags.__class__, enum.Flag.__class__):

			db_flag_array = FlagUtils.enumflag_to_dbflag(flags)
			return (
				db_flag_array[0] if len(db_flag_array) == 1
				else {'$all': db_flag_array}
			)

		elif type(flags) is list:

			or_list = []

			for flag in flags:

				if not isinstance(flag.__class__, enum.Flag.__class__):
					raise ValueError('Illegal type for list member contained in parameter "flags"')

				db_flag_array = FlagUtils.enumflag_to_dbflag(flag)
				or_list.append(
					{field_name: db_flag_array[0]} if len(db_flag_array) == 1
					else {field_name: {'$all': db_flag_array}}
				)

			return (
				{'$or': or_list} if len(or_list) > 1
				else or_list[0][field_name]
			)


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
