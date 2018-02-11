#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/common/LightCurveLoader.py
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 13.01.2018
# Last Modified Date: 25.01.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from ampel.flags.PhotoPointFlags import PhotoPointFlags
from ampel.flags.AlDocTypes import AlDocTypes
from ampel.flags.FlagUtils import FlagUtils
from ampel.base.PhotoPoint import PhotoPoint
from ampel.base.LightCurve import LightCurve
from werkzeug.datastructures import ImmutableList
import logging


class LightCurveLoader:
	"""
	Class with static methods all returning an instance of ampel.pipeline.common.LightCurve
	Either through DB query (load_through_db_query) 
	or through parsing of DB query results (load_from_db_results, load_from_global_db_results)

	Please see method docstrings for more info

	Note: The function 'load_through_db_query' requires static initialization through 
	the method set_mongo to work properly. 
	In short: call LightCurveLoader.set_mongo(...) before using this class and everything will be fine.
	"""


	@classmethod
	def set_mongo(cls, db):
		"""
		sets reference to a pymongo.database.Database instance 
		so that this class can perform DB queries
		"""
		cls.col = db["main"]


	@classmethod
	def load_by_db_query(cls, tran_id, compound_id, logger, read_only=True):
		"""
		Load a lightcurve by performing a DB query and feeding the results 
		to the method 'load_from_db_results' from this class.
		This function returns an instance of ampel.pipeline.common.LightCurve

		Parameters:
		tran_id: transient id 
		compound_id: compound id
		read_only: see docstring of method 'load_from_db_results'
		"""

		# TODO : provide list or cursor as func parameter ?
		# T3 will have larger queries (including t3 results)
		cursor = cls.col.find(
			{	
				"tranId": tran_id, 
				"$or": [
					{"alDocType": AlDocTypes.PHOTOPOINT},
					{"alDocType": AlDocTypes.COMPOUND, "_id": compound_id}
				]
			}
		)

		if (cursor.count()) == 0:
			logger.warn("No LightCurve found for tranId: "+str(tran_id))
			return None

		logger.info(
			"DB query for tranId: " + str(tran_id) +
			" and compoundId: "+ compound_id + 
			" returned " + str(cursor.count()) + " documents"
		)

		return cls.load_from_targeted_db_results(cursor, read_only)


	@classmethod
	def load_from_broad_db_results(cls, doc_list, tran_id, compound_id, read_only=True):
		"""
		Load a lightcurve using db results containing multiple transients.
		This function is mainly used at the T3 level.
		It returns an instance of ampel.pipeline.common.LightCurve

		Parameters:
		doc_list: see docstring of method 'load_from_db_results'
		tran_id: transient id 
		compound_id: compound id
		read_only: see docstring of method 'load_from_db_results'

		Functionwise:
		A filtering of the documents contained in doc_list is performed based on tran_id. 
		All docs with type AlDocTypes.PHOTOPOINT and a single doc with type ==
		AlDocTypes.COMPOUND and with compound ID == 'compound_id' are appended to a list 
		and provided to the method 'load_from_db_results'.
		"""
		sub_list = []

		for doc in doc_list:

			if doc['tranId'] != tran_id:
				continue
			
			if doc["alDocType"] == AlDocTypes.COMPOUND and doc["_id"] == compound_id:
				sub_list.append(doc)

			elif doc["alDocType"] == AlDocTypes.PHOTOPOINT:
				sub_list.append(doc)

		return cls.load_from_targeted_db_results(sub_list, read_only)
		

	@classmethod
	def load_from_targeted_db_results(cls, doc_list, read_only=True):

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

		comp = None
		ppd = dict()
		
		# Loop through query photopoints
		for doc in doc_list:

			if doc["alDocType"] == AlDocTypes.COMPOUND:
				comp = doc

			if doc["alDocType"] == AlDocTypes.PHOTOPOINT:
				ppd[doc['_id']] = PhotoPoint(doc)
			

		# Check wrong result
		if comp == None:
			raise ValueError("Required compound not found")

		# List of *PhotoPoint* object
		pps_list = []

		# Loop through compound photopoints ids and options
		for el in comp['pps']:

			# Get corresponding photopoint instance from ppd dict
			al_pp = ppd[el['pp']]

			# Update list
			pps_list.append(al_pp)

			# If custom options avail (if dict contains more that dict key 'pp')
			if (len(el.keys()) > 1):

				# Update pp options dict and cast internal to ImmutableDict if required
				al_pp.set_policy(el, read_only)

			else:
				if read_only: 
					al_pp.set_policy(read_only=read_only)
				

		return LightCurve(pps_list, read_only)
