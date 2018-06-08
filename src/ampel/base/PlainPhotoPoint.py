#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/base/PlainPhotoPoint.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 13.01.2018
# Last Modified Date: 08.06.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from ampel.base.PhotoData import PhotoData

class PlainPhotoPoint(PhotoData):
	"""
	Wrapper class around a dict instance ususally originating from pymongo DB.
	Please see PhotoData docstring for more info.
	"""

	def get_mag(self):
		"""
		"""
		return self.content[
			self.keywords["mag"]
		]
