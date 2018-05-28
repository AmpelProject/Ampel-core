#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/base/UpperLimit.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 10.05.2018
# Last Modified Date: 29.05.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from ampel.flags.AlDocTypes import AlDocTypes
from ampel.flags.FlagUtils import FlagUtils
from ampel.flags.PhotoFlags import PhotoFlags
from ampel.flags.PhotoPolicy import PhotoPolicy
from types import MappingProxyType


class UpperLimit:
	"""
	Wrapper class around a dict instance ususally originating from pymongo DB.

	This class contains flags, convenience methods and should be able to 
	accomodate different 'upper limit formats' as long as the upper limit 
	content is encoded in a one-dimensional dict. 
	The mapping between the ampel keywords such as 'obs_date' or 'maglim'
	and the keywords of the underlying upper limit dict such as diffmaglim for ZTF-IPAC
	is achieved using the static variable 'static_keywords'

	An instance of this class can be frozen (by setting read_only to True) 
	which should prevent unwilling modifications from happening.
	More precisely, it means:
		-> the internal dict will be casted into an MappingProxyType
		-> a change of any existing internal variable of this instance will not be possible
		-> the creation of new instance variables won't be possible as well
	You can freeze an instance either directly by setting read_only to True in the constructor
	parameters or later by using the method set_policy.
	"""

	static_keywords = {
		'ZTFIPAC': {
			"transient_id" : "tranId",
			"obs_date" : "jd",
			"filter_id" : "fid",
			"maglim" : "diffmaglim"
		}
	}

	@classmethod
	def set_pp_keywords(cls, pp_keywords):
		"""
		Set using ampel config values.
		For ZTFIPAC alerts:
		keywords = {
			"transient_id" : "_id",
			"photopoint_id" : "candid",
			"obs_date" : "jd",
			"filter_id" : "fid",
			"mag" : "magpsf"
		}
		"""
		UpperLimit.static_keywords = pp_keywords


	def __init__(self, db_doc, read_only=True):
		"""
		'db_doc': 
		-> dict instance 
		   (usually resulting from a pymongo DB query)

		'read_only': 
		-> If True, db_doc will be casted into an MappingProxyType 
		   and this class will be frozen
		-> read_only can be set later using the method set_policy
		   (should further modifications be required after class instantiation) 
		"""

		if db_doc["_id"] >= 0:
			raise ValueError("The provided document is not an upper limit")

		# Convert db flag to python enum flag
		self.flags = FlagUtils.dbflag_to_enumflag(
			db_doc['alFlags'], PhotoFlags
		)

		# Check photopoint type and set field keywords accordingly
		if PhotoFlags.INST_ZTF|PhotoFlags.SRC_IPAC in self.flags:
			self.pp_keywords = UpperLimit.static_keywords['ZTFIPAC']

		# Check wether to freeze this instance.
		if read_only:
			self.content = MappingProxyType(db_doc)
			self.__isfrozen = True
		else:
			self.content = db_doc


	def get_policy(self):
		"""
		"""
		if hasattr(self, 'policy_flags'):
			return self.policy_flags
		else:
			return 0


	def set_policy(self, compound_pp_entry=None, read_only=False):
		"""
		"""
		# Check if corrected / alternative magnitudes should be returned
		if compound_pp_entry is not None:
			self.policy_flags = PhotoPolicy(0)
			if 'huzp' in compound_pp_entry:
				self.policy_flags |= PhotoPolicy.USE_HUMBOLDT_ZP

		if read_only:
			self.content = MappingProxyType(self.content)
			self.__isfrozen = True


	def __setattr__(self, key, value):
		"""
		Overrride python's default __setattr__ method to enable frozen instances
		"""
		# '_PhotoPoint__isfrozen' and not simply '__isfrozen' because: 'Private name mangling'
		if getattr(self, "_UpperLimit__isfrozen", None) is not None:
			raise TypeError( "%r is a frozen instance " % self )
		object.__setattr__(self, key, value)


	def get_value(self, field_name):
		"""
		"""
		field_name = self.pp_keywords[field_name] if field_name in self.pp_keywords else field_name
		return self.content[field_name]


	def get_tuple(self, field1_name, field2_name):
		"""
		"""
		field1_name = self.pp_keywords[field1_name] if field1_name in self.pp_keywords else field1_name
		field2_name = self.pp_keywords[field2_name] if field2_name in self.pp_keywords else field2_name
		return (self.content[field1_name], self.content[field2_name])

	
	def has_flags(self, arg_flags):
		return arg_flags in self.flags


	def has_parameter(self, field_name):
		"""
		"""
		return (
			field_name in self.content if field_name not in self.pp_keywords
			else self.pp_keywords[field_name] in self.content 
		)


	def get_mag_lim(self):
		"""
		"""
		if hasattr(self, 'policy_flags'):
			raise NotImplementedError("Not implemented yet")

		return self.content[
			self.pp_keywords["mag"]
		]


	def get_id(self):
		"""
		"""
		return self.content["_id"]


	def get_obs_date(self):
		"""
		"""
		return self.content[
			self.pp_keywords["obs_date"]
		]


	def get_filter_id(self):
		"""
		"""
		return self.content[
			self.pp_keywords["filter_id"]
		]
