#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/common/PhotoPoint.py
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 13.01.2018
# Last Modified Date: 16.01.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from ampel.flags.PhotoPointFlags import PhotoPointFlags
from ampel.flags.AlDocTypes import AlDocTypes
from ampel.flags.FlagUtils import FlagUtils
from ampel.flags.PhotoPointPolicy import PhotoPointPolicy
from werkzeug.datastructures import ImmutableDict


class PhotoPoint:
	"""
		Documentation will follow
	"""

	static_keywords = {
		'ZTF': {
			'IPAC': {
				"transient_id" : "_id",
				"photopoint_id" : "candid",
				"obs_date" : "jd",
				"filter_id" : "fid",
				"mag" : "magpsf",
				"magerr": "sigmapsf",
				"dec" : "dec",
				"ra" : "ra"
			}
		}
	}

	@classmethod
	def set_pp_keywords(cls, pp_keywords):
		"""
			Set using ampel config values.
			For ZTF IPAC alerts:
			keywords = {
				"transient_id" : "_id",
				"photopoint_id" : "candid",
				"obs_date" : "jd",
				"filter_id" : "fid",
				"mag" : "magpsf"
			}
		"""
		PhotoPoint.static_keywords = pp_keywords


	def __init__(self, db_doc):
		"""
		"""

		if db_doc["alDocType"] != AlDocTypes.PHOTOPOINT:
			raise ValueError("The provided document is not a photopoint")

		# Convert db flag to python enum flag
		self.flags = FlagUtils.dbflag_to_enumflag(
			db_doc['alFlags'], PhotoPointFlags
		)

		# Check photopoint type and set field keywords accordingly
		if PhotoPointFlags.INST_ZTF in self.flags:
			if PhotoPointFlags.SRC_IPAC in self.flags:
				self.pp_keywords = PhotoPoint.static_keywords['ZTF']['IPAC']
			else:
				raise NotImplementedError("Not implemented yet")
		else:
			raise NotImplementedError("Not implemented yet")


		self.content = db_doc


	def set_policy(self, options=None, read_only=False):

		# Check if corrected / alternative magnitudes should be returned
		if options is not None:
			self.policy_flags = PhotoPointPolicy(0)
			if 'wzm' in options:
				self.policy_flags |= PhotoPointPolicy.USE_WEIZMANN_SUB
			if 'huzp' in options:
				self.policy_flags |= PhotoPointPolicy.USE_HUMBOLDT_ZP

		if read_only:
			self.content = ImmutableDict(self.content)
			self.__isfrozen = True


	def __setattr__(self, key, value):
		"""
		Overrride python's default __setattr__ method to enable frozen instances
		"""
		# '_PhotoPoint__isfrozen' and not simply '__isfrozen' because: 'Private name mangling'
		if getattr(self, "_PhotoPoint__isfrozen", None) is not None:
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
		if field_name in self.pp_keywords:
			field_name = self.pp_keywords[field_name]
			
		return field_name in self.content


	def get_mag(self):
		"""
		"""

		if hasattr(self, 'policy_flags'):
			raise NotImplementedError("Not implemented yet")

		return self.content[
			self.pp_keywords["mag"]
		]


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


	def get_photopoint_id(self):
		"""
		"""
		return self.content["_id"]


	def get_ra(self):
		"""
		"""
		return self.content[
			self.pp_keywords["ra"]
		]


	def get_dec(self):
		"""
		"""
		return self.content[
			self.pp_keywords["dec"]
		]
