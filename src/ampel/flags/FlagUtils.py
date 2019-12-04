#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/core/flags/FlagUtils.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 14.12.2017
# Last Modified Date: 24.02.2019
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

#from bson import Binary
from ampel.config.AmpelConfig import AmpelConfig
from ampel.common.AmpelUtils import AmpelUtils
from ampel.config.t3.AnyOf import AnyOf
from ampel.config.t3.AllOf import AllOf
from ampel.config.t3.OneOf import OneOf

class FlagUtils():


	@staticmethod
	def has_any(enum_flag, *flags):
		for flag in flags:
			if flag in enum_flag:
				return True
		return False


	@classmethod
	def hash_schema(cls, arg):
		"""
		Converts dict schema containing str representation of tags into 
		a dict schema containing hashed values (int64).

		:param arg: schema dict. See :obj:`QueryMatchSchema <ampel.db.query.QueryMatchSchema>` \
		docstring for more info regarding the used syntax.
		:type arg: str, dict, \
			:py:class:`AllOf <ampel.config.t3.AllOf>`, \
			:py:class:`AnyOf <ampel.config.t3.AnyOf>`, \
			:py:class:`OneOf <ampel.config.t3.OneOf>`,

		:returns: new schema dict where tag elements are integers
		:rtype: dict
		"""

		out={}
		
		if isinstance(arg, str):
			return AmpelConfig._tags[arg]

		if type(arg) in (AllOf, AnyOf, OneOf):
			arg = arg.dict()

		if isinstance(arg, dict):

			if "anyOf" in arg:
				if AmpelUtils.check_seq_inner_type(arg['anyOf'], str):
					out['anyOf'] = [AmpelConfig._tags[el] for el in arg['anyOf']]
				else:
					out['anyOf'] = []
					for el in arg['anyOf']:
						if isinstance(el, str):
							out['anyOf'].append(AmpelConfig._tags[el])
						elif isinstance(el, dict):
							if 'allOf' not in el:
								raise ValueError("Unsupported format (1)")
							out['anyOf'].append({'allOf': [AmpelConfig._tags[ell] for ell in el['allOf']]})
						else:
							raise ValueError("Unsupported format (type: %s)" % type(el))
	
			elif 'allOf':
				out['allOf'] = [AmpelConfig._tags[el] for el in arg['allOf']]

			elif 'oneOf':
				out['oneOf'] = [AmpelConfig._tags[el] for el in arg['oneOf']]
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
