#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/config/ZIInputParameter.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 15.03.2018
# Last Modified Date: 16.03.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from ampel.flags.PhotoPointFlags import PhotoPointFlags

class ZIInputParameter:
	"""
	"""

	def __init__(self, db_doc):
		"""
		"""
		if db_doc is None:
			raise ValueError("Missing parameter 'db_doc'")

		if db_doc['instrument'] != "ZTF":
			raise ValueError("Parameter 'instrument' must be 'ZTF'")

		if db_doc['alerts'] != "IPAC":
			raise ValueError("Parameter 'alerts' must be 'IPAC'")

		self.parameters = db_doc['parameters']

		if not "ZTFPartner" in self.parameters:
			raise ValueError("Parameter 'ZTFPartner' is missing")

		# Default autoComplete value is False
		if not "autoComplete" in self.parameters:
			self.parameters["autoComplete"] = False

		# Default updatedHUZP value is False
		if not "updatedHUZP" in self.parameters:
			self.parameters["updatedHUZP"] = False

		if not "weizmannSub" in self.parameters:
			self.parameters["weizmannSub"] = False

		self.pps_must_flags = PhotoPointFlags.INST_ZTF

		if self.parameters["ZTFPartner"] is False:
			self.pps_must_flags |= PhotoPointFlags.ZTF_PUBLIC


	def get_parameter(self, name):
		"""	
		Dict path lookup shortcut function
		"""	
		if not name in self.parameters:
			return None

		return self.parameters[name]

	
	def get_parameters(self):
		return self.parameters


	def ztf_partner(self):
		return self.parameters["ZTFPartner"]


	def get_pps_must_flags(self):
		return self.pps_must_flags


	def auto_complete(self):
		return self.parameters["autoComplete"]
