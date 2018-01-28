#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/t3/Transient.py
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 13.01.2018
# Last Modified Date: 24.01.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from ampel.pipeline.logging.LoggingUtils import LoggingUtils
from ampel.base.PhotoPoint import PhotoPoint
from ampel.base.LightCurve import LightCurve
from ampel.flags.PhotoPointFlags import PhotoPointFlags
from ampel.flags.AlDocTypes import AlDocTypes
from ampel.flags.FlagUtils import FlagUtils
from werkzeug.datastructures import ImmutableList
import logging


class Transient:
	"""
		Class containing a master LightCurve instance (usually the 'latest') 
		or in certain case a collection of LightCurve instances.
		And a few convenience methods to return values embedded in the collection.
	"""

	col = None


	@classmethod
	def set_mongo(cls, db):
		"""
		db: instance of pymongo.database.Database
		"""
		cls.col = db["main"]


	def __init__(self, tran_id, logger=None):
		"""
		Parameters:
		tran_id: transient id (string)
		compound_id: compound id
		"""

		self.tran_id = tran_id
		self.compounds = {}
		self.photopoints = {}
		self.logger = LoggingUtils.get_logger() if logger is None else logger


	def set_compound(self, compound):
		self.compounds[compound.get_id()] = compound


	def set_photopoints(self, list_of_photopoints):
		for pp in list_of_photopoints:
			self.photopoints[pp['_id']] = pp

	
	def get_latest_compound_id(self):
		pass


	def __setattr__(self, key, value):
		"""
		Overrride python's default __setattr__ method to enable frozen instances
		"""
		# '_LightCurve__isfrozen' and not simply '__isfrozen' due to 'Private name mangling'
		if getattr(self, "_Transient__isfrozen", None) is not None:
			raise TypeError( "%r is a frozen instance " % self )
		object.__setattr__(self, key, value)


	def load_all_lightcurves(self, doc_list, read_only=True):

		"""
		doc_list: 
		list of documents retrieved from DB.
		Typically, it could be: 
			list(
				db_collection.find({...})
			)

		read_only: 
		if True, the LightCurve instance returned by this method will be:
			* a frozen class
			* containing a ImmutableList of PhotoPoint
			* whereby each PhotoPoint is a frozen class as well
			* and each PhotoPoint dict content is an ImmutableDict
		"""

		comps = []
		
		# Loop through query photopoints
		for doc in doc_list:

			if doc["alDocType"] == AlDocTypes.COMPOUND:
				comps.append(doc)

			if doc["alDocType"] == AlDocTypes.PHOTOPOINT:
				self.al_pps[doc['_id']] = PhotoPoint(doc)
				if read_only:
					self.al_pps[doc['_id']].set_policy(read_only=read_only)
			

		for comp in comps:

			# List of *PhotoPoint* object
			pps_list = []
	
			# Loop through compound photopoints ids and options
			for el in comp['pps']:
	
				# Get corresponding photopoint instance from ppd dict
				al_pp = self.al_pps[el['pp']]
	
				# Update list
				pps_list.append(al_pp)
	
				# If custom options avail (if dict contains more that dict key 'pp')
				if (len(el.keys()) > 1):
	
					# Update pp options dict and cast internal to ImmutableDict if required
					al_pp.set_policy(el)

			self.lc[comp['id']] = LightCurve(pps_list, read_only)
