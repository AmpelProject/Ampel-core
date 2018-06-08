#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/base/ScienceRecord.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 13.01.2018
# Last Modified Date: 08.06.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from ampel.flags.AlDocTypes import AlDocTypes
from ampel.flags.T2RunStates import T2RunStates
from ampel.base.Frozen import Frozen
from bson import ObjectId

class ScienceRecord(Frozen):
	"""
	Wrapper class around a dict instance ususally originating from pymongo DB.
	"""

	def __init__(self, tran_id, t2_unit_id, compound_id, results, info=None, read_only=True):
		"""
		tran_id: transient id (string or int)
		t2_unit_id: T2 unit id (string)
		compound_id: bytes
		results: list of dict instances,
		info: dict instance
		read_only: true->freeze this instance
		"""

		self.tran_id = tran_id
		self.t2_unit_id = t2_unit_id
		self.compound_id = compound_id
		self.results = results
		self.info = info

		# Check wether to freeze this instance.
		if read_only:
			self.__isfrozen = True


	def get_results(self):
		""" """
		return self.results


	def get_t2_unit_id(self):
		""" """
		return self.t2_unit_id


	def get_compound_id(self):
		""" """
		return self.compound_id


	def has_error(self):
		return self.info.get('hasError')
