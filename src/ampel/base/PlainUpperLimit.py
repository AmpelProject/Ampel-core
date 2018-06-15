#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/base/PlainUpperLimit.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 10.05.2018
# Last Modified Date: 08.06.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from ampel.base.PhotoData import PhotoData

class PlainUpperLimit(PhotoData):
	"""
	Wrapper class around a dict instance ususally originating from pymongo DB.
	Please see PhotoData docstring for more info.
	"""

	def get_mag_lim(self):
		"""
		"""
		return self.content[
			self.keywords["maglim"]
		]
