#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/base/PhotoPoint.py
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 13.01.2018
# Last Modified Date: 15.02.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from ampel.flags.AlDocTypes import AlDocTypes
from ampel.flags.ChannelFlags import ChannelFlags
from ampel.flags.T2RunStates import T2RunStates
from ampel.flags.FlagUtils import FlagUtils
from bson import ObjectId

class ScienceRecord:
	"""
	Wrapper class around a dict instance ususally originating from pymongo DB.
	"""

	def __init__(self, db_doc, save_channels=False, read_only=True):
		"""
		"""
		if db_doc["alDocType"] != AlDocTypes.T2RECORD:
			raise ValueError("The provided document is not a science record")

		if save_channels:
			self.channels = FlagUtils.dbflag_to_enumflag(
				db_doc['channels'], ChannelFlags
			)

		self.tran_id = db_doc['tranId']
		self.compound_id = db_doc['compoundId']
		self.parameter_id = db_doc['paramId']
		self.runnable_id = db_doc['t2Runnable']
		self.run_state = db_doc['runState']
		self.results = db_doc['results'] if 'results' in db_doc else None
		self.generation_time = ObjectId(db_doc['_id']).generation_time

		# Check wether to freeze this instance.
		if read_only:
			self.__isfrozen = True


	def get_results(self):
		""" """
		return self.results if hasattr(self, 'results') else None


	def get_t2_runnable_id(self):
		""" """
		return self.runnable_id


	def get_compound_id(self):
		""" """
		return self.compound_id


	def has_error(self):
		return True if self.run_state == T2RunStates.ERROR else False


	def __setattr__(self, key, value):
		"""
		Overrride python's default __setattr__ method to enable frozen instances
		"""
		# '_ScienceRecord__isfrozen' and not simply '__isfrozen' because: 'Private name mangling'
		if getattr(self, "_ScienceRecord__isfrozen", None) is not None:
			raise TypeError( "%r is a frozen instance " % self )

		object.__setattr__(self, key, value)
