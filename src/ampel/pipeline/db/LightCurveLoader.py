#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/db/LightCurveLoader.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 13.01.2018
# Last Modified Date: 04.05.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from ampel.flags.AlDocTypes import AlDocTypes
from ampel.base.PhotoPoint import PhotoPoint
from ampel.base.LightCurve import LightCurve
from ampel.pipeline.logging.LoggingUtils import LoggingUtils
from ampel.pipeline.db.DBResultOrganizer import DBResultOrganizer


class LightCurveLoader:
	"""
	Class with methods each returning an instance of ampel.base.LightCurve
	Either through DB query (load_through_db_query) or through parsing of DB query results 
	"""


	def __init__(self, col, read_only=True, logger=None):
		"""
		Parameters:
		-----------
		col: instance of pymongo.collection.Collection 
		read_only: if True, the LightCurve instance returned by the methods of this class will be:
			* a frozen class
			* containing a ImmutableList of PhotoPoint
			* whereby each PhotoPoint is a frozen class as well
			* and each PhotoPoint dict content is an ImmutableDict
		"""
		self.col = col 
		self.logger = LoggingUtils.get_logger() if logger is None else logger
		self.read_only = read_only


	def load_from_db(self, tran_id, compound_id):
		"""
		Load a lightcurve by performing a DB query and feeding the results 
		to the method 'load_from_db_results' from this class.
		This function returns an instance of ampel.base.LightCurve

		Parameters:
		tran_id: transient id (string)
		compound_id: compound id (string)
		"""

		# TODO : provide list or cursor as func parameter ?
		# T3 will have larger queries (including t3 results)
		cursor = self.col.find(
			{	
				"tranId": tran_id, 
				"$or": [
					{"alDocType": AlDocTypes.PHOTOPOINT},
					{	
						"alDocType": AlDocTypes.COMPOUND, 
						"_id": compound_id
					}
				]
			}
		)

		if cursor.count() == 0:
			self.logger.warn("No LightCurve found for tranId: %s " % tran_id)
			return None

		self.logger.info(
			"DB query for tranId: %s and compoundId: %s returned %i documents" %
			(tran_id, compound_id, cursor.count())
		)

		results = list(cursor)

		pps, comp = DBResultOrganizer.get_lightcurve_constituents(
			results, compound_id=compound_id
		)

		return self.load_using_results(pps, comp)


	def load_using_results(self, photopoints, compound_dict, frozen_pps_dict=None):
		"""
		Creates and returns an instance of ampel.base.LightCurve using db results.
		This function is used at both T2 and T3 levels 
		(T3 would typically set the optional 'target' parameter, see below for more info).

		Required parameters:
		--------------------

		'doc_list': 
			-> List of documents or pymongo cursor retrieved from mongo DB.
			-> Typically, it could be: db_collection.find({...}) or list(db_collection.find({...})) 

		Optional parameters:
		--------------------

		'target':
			-> Mostly used at the T3 level were broad DB queries are performed 
			  (T2 is more likely to execute targeted queries)
			-> Must be a dict containing both the keys 'tran_id' and 'compound_id'
			-> a filtering of the documents in doc_list is performed based on tran_id. 
			   Docs with type AlDocTypes.PHOTOPOINT and a single doc with 
			   type == AlDocTypes.COMPOUND and with compound ID == 'compound_id' are used
			   as basis for instancianting the LightCurve object

		'frozen_pps_dict': 
			Dict that can be provided for optimization purposes, which should contain frozen PhotoPoint instances.
			-> key: photopoint id 
			-> value: is the corresponding PhotoPoint instance
			-> can be empty (will then be populated)
			-> will be populated in case new photopoint instance(s) is(are) created (i.e pp id not in dict)
			-> PhotoPoint instances are 're-used' when already existing. 
			  (rather than instanciating a new PhotoPoint obj for every LightCurve object)
			-> contains only PhotoPoint instances without custom policy
			-> will be used only if read_only is True (see class constructor)
		"""

		# Robustness check 
		if compound_dict is None:
			raise ValueError("Required parameter 'compound_dict' cannot be None")

		if photopoints is None:
			raise ValueError("Required parameter 'photopoints' cannot be None")

		if type(photopoints) is dict:
			pass

		elif type(photopoints) is list:

			pp_list = photopoints
			photopoints = {}

			# Loop through photopoints 
			for pp in pp_list:

				# Build dict
				photopoints[pp['_id']] = pp

		else:
			raise ValueError("'photopoints' argument must be an instance of list or dict")


		# List of *PhotoPoint* instances
		al_pps_list = []

		# Loop through compound elements
		for el in compound_dict['comp']:

			# Get corresponding photopoint content (dict) from res_pps dict
			try:
				pp_dict = photopoints[el['pp']]
			except KeyError:

				self.logger.warn("KeyError exception occured while accessing photopoint %i" % el['pp'])
				self.logger.info("Trying to recover from this error")

				# TODO: populate 'troubleshoot collection'
				cursor = self.col.find(
					{
						"_id": el['pp'], 
						"alDocType": AlDocTypes.PHOTOPOINT
					}
				)

				if (cursor.count()) == 0:
					self.logger.error("PhotoPoint with id %i not found" % el['pp'])
					raise ValueError("PhotoPoint with id %i not found" % el['pp'])

				pp_dict = next(cursor)


			# If custom options avail (if dict contains more than the dict key 'pp')
			if (len(el.keys()) > 1):
				
				# Create photopoint wrapper instance
				al_pp = PhotoPoint(pp_dict, read_only=False)

				# Update pp options dict and cast internal to ImmutableDict if required
				al_pp.set_policy(el, self.read_only)

			# Photopoint defined in the compound has no special policy, i.e len(el.keys()) == 1
			else:

				# A dict containing frozen PhotoPoint instances might have been provided
				if self.read_only and frozen_pps_dict is not None: 

					# In this case, rather than instanciating a new PhotoPoint obj, we use pre-existing one
					if pp_dict['_id'] in frozen_pps_dict:
						al_pp = frozen_pps_dict[pp_dict['_id']]

					# update frozen_pps_dict dict if pp id not found in the dict keys
					else:
						al_pp = PhotoPoint(pp_dict, self.read_only)
						frozen_pps_dict[pp_dict['_id']] = al_pp
				else:
					al_pp = PhotoPoint(pp_dict, self.read_only)

			# Update internal list of PhotoPoint instances
			al_pps_list.append(al_pp)

		return LightCurve(compound_dict, al_pps_list, self.read_only)
