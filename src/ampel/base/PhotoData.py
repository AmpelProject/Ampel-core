#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/base/PhotoData.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 13.01.2018
# Last Modified Date: 08.06.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from ampel.flags.PhotoFlags import PhotoFlags
from ampel.base.Frozen import Frozen
from types import MappingProxyType

class PhotoData(Frozen):
	"""
	Photometric data class. 
	Known child implementations: PhotoPoint, UpperLimit.

	This class convenience methods and should be able to 
	accomodate different 'formats' as long as the photometric 
	content is encoded in a one-dimensional dict. 
	The mapping between - let's call them ampel keywords such as 'mag'
	and the keywords of the underlying photopoint dict (for ZTF-IPAC 'magpsf')
	is achieved using the static variable 'default_keywords'
	"""

	default_keywords = {
		'ZTFIPAC': {
			"obs_date": "jd",
			"filter_id": "fid",
			"mag": "magpsf",
			"maglim": "diffmaglim",
			"magerr": "sigmapsf",
			"dec": "dec",
			"ra": "ra",
		}
	}

	@classmethod
	def set_keywords(cls, keywords):
		""" Usually set using ampel config values. """
		PhotoData.default_keywords = keywords


	def __init__(self, content, flags=None, read_only=True):

		# Check flags and set field keywords accordingly
		if flags is not None and PhotoFlags.INST_ZTF|PhotoFlags.SRC_IPAC in flags:
			self.keywords = PhotoData.default_keywords['ZTFIPAC']
		else:
			self.keywords = {}

		self.flags = flags

		# Check wether to freeze this instance.
		if read_only:
			self.content = MappingProxyType(content)
			self.__isfrozen = True
		else:
			self.content = content


	def get_value(self, field_name):
		"""
		"""
		return self.content[
			self.keywords[field_name] if field_name in self.keywords	
			else field_name
		]


	def get_tuple(self, field1_name, field2_name):
		""" """
		return (
			self.content[
				self.keywords[field1_name] if field1_name in self.keywords 
				else field1_name
			], 
			self.content[
				self.keywords[field2_name] if field2_name in self.keywords 
				else field2_name
			]
		)
	

	def has_flags(self, arg_flags):
		"""
		arg_flags: can be:
			* an enumflag: has_flags() will return True or False
			* a list of enumflags: has_flags() will return a list containing booleans
		"""
		if self.flags is None:
			return False

		if type(arg_flags) is list:
			return [f for f in arg_flags in self.flags if f in self.flags]

		return arg_flags in self.flags


	def has_parameter(self, field_name):
		"""
		"""
		return (
			field_name in self.content if field_name not in self.keywords
			else self.keywords[field_name] in self.content 
		)


	def get_id(self):
		"""
		"""
		return self.content["_id"]
