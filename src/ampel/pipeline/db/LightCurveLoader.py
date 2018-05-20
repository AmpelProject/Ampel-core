#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/db/LightCurveLoader.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 13.01.2018
# Last Modified Date: 20.05.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from ampel.flags.AlDocTypes import AlDocTypes
from ampel.base.PhotoPoint import PhotoPoint
from ampel.base.UpperLimit import UpperLimit
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
			* containing a immutable list (tuple) of PhotoPoint
			* whereby each PhotoPoint is a frozen class as well
			* and each PhotoPoint dict content is an immutable dict
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
					{
						"alDocType": {
							"$in": [AlDocTypes.PHOTOPOINT, AlDocTypes.UPPERLIMIT]
						}
					},
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

		pps, uls, comp = DBResultOrganizer.get_lightcurve_constituents(
			cursor, compound_id=compound_id
		)

		return self.load_using_results(pps, uls, comp)


	def load_using_results(self, ppd_list, uld_list, compound_dict, frozen_pps_dict=None):
		"""
		Creates and returns an instance of ampel.base.LightCurve using db results.
		This function is used at both T2 and T3 levels 

		Required parameters:
		--------------------
		ppd_list: list of photopoint dict instances loaded from DB
		uld_list: list of upper limit dict instances loaded from DB
		compound_dict: compound dict instance loaded from DB

		Optional parameter:
		-------------------
		'frozen_pps_dict': 
		Dict that can be provided for optimization purposes, which should contain frozen PhotoPoint instances.
		-> key: photopoint or upperlimit id 
		-> value: is the corresponding PhotoPoint or UpperLimit instance
		-> dict can be empty (will then be populated)
		-> will be populated in case new photopoint instance(s) is(are) to be created (i.e pp id not in dict)
		-> PhotoPoint/UpperLimit instances are 're-used' when already existing. 
		  (rather than instantiating a new PhotoPoint obj for every LightCurve object)
		-> contains only PhotoPoint/UpperLimit instances without custom policy
		-> will be used only if read_only is True (see LightCurveLoader constructor)
		"""

		# Robustness check 
		if compound_dict is None:
			raise ValueError("Required parameter 'compound_dict' cannot be None")

		if ppd_list is None or type(ppd_list) is not list:
			raise ValueError("Parameter 'ppd_list' must be a list")

		if uld_list is None:
			uld_list = []

		# List of PhotoPoint object instances
		ppo_list = []

		# List of UpperLimit object instances
		ulo_list = []

		# Loop through compound elements
		for el in compound_dict['comp']:

			# Get corresponding photopoint / upper limit
			if 'pp' in el:

				photo_dict = next((pp for pp in ppd_list if pp["_id"] == el["pp"]), None)	

				if photo_dict is None:

					self.logger.warn("Photo point %i not found" % el['pp'])
					self.logger.info("Trying to recover from this error")

					# TODO: populate 'troubles collection'
					cursor = self.col.find(
						{
							"_id": el['pp'], 
							"alDocType": AlDocTypes.PHOTOPOINT
						}
					)
	
					if (cursor.count()) == 0:
						self.logger.error("PhotoPoint with id %i not found" % el['pp'])
						raise ValueError("PhotoPoint with id %i not found" % el['pp'])

					photo_dict = next(cursor)

			else:

				photo_dict = next((ul for ul in uld_list if ul["_id"] == el["ul"]), None)	
				if photo_dict is None:
					raise ValueError("Upper limit %i not found" % el['ul'])


			# If custom options avail (if dict contains more than the dict key 'pp')
			if (len(el.keys()) > 1):
				
				obj = (
					# Create photopoint wrapper instance
					PhotoPoint(photo_dict, read_only=False) if 'pp' in el 
					# Create upperlimit wrapper instance
					else UpperLimit(photo_dict, read_only=False)
				)

				# Update pp options dict and cast internal to immutable dict if required
				obj.set_policy(el, self.read_only)

			# Photopoint defined in the compound has no special policy, i.e len(el.keys()) == 1
			else:

				# A dict containing frozen PhotoPoint instances might have been provided
				if self.read_only and frozen_pps_dict is not None: 

					# In this case, rather than instantiating a new PhotoPoint obj, we use pre-existing one
					if photo_dict['_id'] in frozen_pps_dict:
						obj = frozen_pps_dict[photo_dict['_id']]

					# update frozen_pps_dict dict if pp id not found in the dict keys
					else:
						obj = (
							PhotoPoint(photo_dict, self.read_only) if 'pp' in el 
							else UpperLimit(photo_dict, self.read_only)
						)
						frozen_pps_dict[photo_dict['_id']] = obj
				else:
					obj = (
						PhotoPoint(photo_dict, self.read_only) if 'pp' in el 
						else UpperLimit(photo_dict, self.read_only)
					)

			# Update internal list of PhotoPoint/UpperLimit instances
			if 'pp' in el:
				ppo_list.append(obj)
			else:
				ulo_list.append(obj)

		return LightCurve(compound_dict, ppo_list, ulo_list, self.read_only)
