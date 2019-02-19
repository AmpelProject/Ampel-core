#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/core/flags/FlagUtils.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 14.12.2017
# Last Modified Date: 19.02.2019
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import enum, binascii
from bson import Binary
from ampel.pipeline.common.AmpelUtils import AmpelUtils
from ampel.pipeline.config.t3.AnyOf import AnyOf
from ampel.pipeline.config.t3.AllOf import AllOf
from ampel.pipeline.config.t3.OneOf import OneOf

class FlagUtils():

	saved_crcs = {}

	@staticmethod
	def has_any(enum_flag, *flags):
		for flag in flags:
			if flag in enum_flag:
				return True
		return False


	@classmethod
	def enumflag_to_dbtags(cls, enum_flag):
		"""
		Converts an enum flag instance such as: <ZICompoundFlag.SURVEY_ZTF|HAS_CUSTOM_POLICIES: 192>
		into an array of ints: [2815847261, 131172144]
		whereby each list element in the output list is the computed crc32 of a flag member name
			In []: binascii.crc32(bytes("SURVEY_ZTF", "ascii"))
			Out[]: 131172144
		"""
		if enum_flag.__class__ not in cls.saved_crcs:
			cls._save_crcs(enum_flag.__class__)

		return [
			cls.saved_crcs[enum_flag.__class__][el[0]] 
			for el in enum_flag.__class__.__members__.items() 
			if el[1] in enum_flag
		]


	@classmethod
	def dbtags_to_enumflag(cls, db_flag, EnumClass):
		"""
		Converts: [2815847261, 131172144]
		Into: <ZICompoundFlag.SURVEY_ZTF|HAS_CUSTOM_POLICIES: 192>
		"""
		enum_flag = EnumClass(0)

		if EnumClass not in cls.saved_crcs:
			cls._save_crcs(EnumClass)

		for el in EnumClass.__members__.items():
			if cls.saved_crcs[EnumClass][el[0]] in db_flag:
				enum_flag |= el[1]

		return enum_flag


	@classmethod
	def _save_crcs(cls, EnumClass):
		cls.saved_crcs[EnumClass] = {}
		for el in EnumClass.__members__.items():
			cls.saved_crcs[EnumClass][el[0]] = binascii.crc32(bytes(el[0], 'ascii'))


	@classmethod
	def to_dbtags_schema(cls, arg, EnumClass):
		"""
		Converts dict schema containing str representation of enum class members, \
		into integers whose values are the index position of each member within the enum. \
		We do a such conversion to ensure storing flags into mongoDB remains \
		easy even if the enum hosts more than 64 members (in this case, \
		storing flag values would require a cumbersome conversion to BinData)

		:param arg: schema dict. See :obj:`QueryMatchSchema <ampel.pipeline.db.query.QueryMatchSchema>` \
		for syntax detail.
		:type arg: str, dict, :py:class:`AllOf <ampel.pipeline.config.t3.AllOf>`, \
			:py:class:`AnyOf <ampel.pipeline.config.t3.AnyOf>`, \
			:py:class:`OneOf <ampel.pipeline.config.t3.OneOf>`,
		:param EnumClass: enum class (not instance) ex: ampel.base.flags.TransientFlags

		:returns: new schema dict where flag elements are integer
		:rtype: dict
		"""

		if EnumClass not in cls.saved_crcs:
			cls._save_crcs(EnumClass)

		out={}
		
		if isinstance(arg, str):
			return cls.saved_crcs[EnumClass][arg]

		if type(arg) in (AllOf, AnyOf, OneOf):
			arg = arg.dict()

		if isinstance(arg, dict):

			if "anyOf" in arg:
				if AmpelUtils.check_seq_inner_type(arg['anyOf'], str):
					out['anyOf'] = [cls.saved_crcs[EnumClass][el] for el in arg['anyOf']]
				else:
					out['anyOf'] = []
					for el in arg['anyOf']:
						if isinstance(el, str):
							out['anyOf'].append(cls.saved_crcs[EnumClass][el])
						elif isinstance(el, dict):
							if 'allOf' not in el:
								raise ValueError("Unsupported format (1)")
							out['anyOf'].append({'allOf': [cls.saved_crcs[EnumClass][ell] for ell in el['allOf']]})
						else:
							raise ValueError("Unsupported format (type: %s)" % type(el))
	
			elif 'allOf':
				out['allOf'] = [cls.saved_crcs[EnumClass][el] for el in arg['allOf']]

			elif 'oneOf':
				out['oneOf'] = [cls.saved_crcs[EnumClass][el] for el in arg['oneOf']]
		else:
			raise ValueError("Unsupported format (%s)" % type(arg))
		
		return out


	#@staticmethod
	#def int_to_bindata(int_arg):
	#	"""
	#		converts a python integer number (unlimited length) into a MongoDB BSON data type 'BinData'. 
	#		The used subtype 0 (\\x00): "Generic binary subtype"
	#		The int to bytes conversion uses the little Endian byte ordering 
	#		(most significant byte is at the end of the byte array)
	#	"""
	#	return Binary(
	#		int_arg.to_bytes(
	#			(int_arg.bit_length() + 7) // 8,
	#			'little'
	#		),
	#		0
	#	)

	#@staticmethod
	#def bindata_bytes_to_int(bin_data_bytes):
	#	"""
	#		converts a BSON data type 'BinData' (subtype 0) into a python integer number
	#		The little Endian byte ordering is used 
	#		(most significant byte is at the end of the byte array)
	#	"""
	#	return int.from_bytes(bin_data_bytes, byteorder='little')
