#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/t3/TCLoader.py
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 22.02.2018
# Last Modified Date: 22.02.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from ampel.pipeline.logging.LoggingUtils import LoggingUtils
from ampel.flags.AlDocTypes import AlDocTypes
from ampel.flags.ChannelFlags import ChannelFlags
from ampel.flags.FlagUtils import FlagUtils
from ampel.pipeline.t3.TransientLoader import TransientLoader
import logging


class CollectionProvider:
	"""
	Transient collection loader
	"""

	def __init__(self, db, logger=None, collection="main"):
		"""
		"""
		self.logger = LoggingUtils.get_logger() if logger is None else logger
		self.col = db[collection]
		self.tl = TransientLoader(db)


	def set_time_range(self, date_from, date_until):
		""" """
		self.date_from = date_from
		self.date_until = date_until


	def set_instrument(self, date_from, date_until):
		""" """
		self.date_from = date_from
		self.date_until = date_until



	def set_back_time(self, seconds):
		""" """
		self.back_time = seconds


	def set_channels(self, channel_flags):
		""" """
		self.channel_flags = channel_flags


	def load_latest_state(self):
		""" """
		self.latest_state = True


	def load_all_states(self):
		""" """
		self.latest_state = False


	def set_page_size(self, page_size):
		""" """
		self.page_size = page_size


	def set_tran_ids(self, tran_ids):
		""" """
		self.tran_ids = tran_ids


	def get_collection(self):
		""" """
		pass


	def has_next(self):
		""" """
		pass


	@staticmethod	
	def get_transients_with_mixed_compounds(col, tran_ids, channel_flags=None):
		"""
		Returns a set of transientIds from transients having compounds created at 
		other level than at T0. 
		For those, the compound with the lengthiest number of photopoints 
		does not necessarily correspond to the latest compound.
		"""
		if type(tran_ids) is not list:
			raise ValueError("Parameter 'tran_ids' must be a list of strings")

		match_dict = {
			'tranId': {'$in': tran_ids},
			'alDocType': AlDocTypes.COMPOUND, 
			'tier': {'$ne': 0}
		}

		if channel_flags is not None:
			FlagUtils.enum_flags_to_dbquery(channel_flags, match_dict, 'channels')

		res = col.find(
			match_dict, 
			{'_id':0, 'tranId':1}
		)

		ret = set()
		for doc in res:
			ret.add(doc['tranId'])

		return ret
