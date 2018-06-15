#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/base/UpperLimit.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 10.05.2018
# Last Modified Date: 08.06.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from ampel.flags.PhotoPolicy import PhotoPolicy
from ampel.base.PhotoData import PhotoData
from types import MappingProxyType

class UpperLimit(PhotoData):
	"""
	Wrapper class around a dict instance ususally originating from pymongo DB.
	Please see PhotoData docstring for more info.
	You can freeze an instance either directly by setting read_only to True in the constructor
	parameters or later by using the method set_policy.
	"""

	def get_mag_lim(self):
		"""
		"""
		if hasattr(self, 'policy_flags'):
			raise NotImplementedError("Not implemented yet")

		return self.content[
			self.keywords["mag"]
		]


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
