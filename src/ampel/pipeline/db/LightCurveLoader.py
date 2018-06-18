#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/db/LightCurveLoader.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 13.01.2018
# Last Modified Date: 18.06.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from ampel.flags.AlDocTypes import AlDocTypes
from ampel.flags.FlagUtils import FlagUtils
from ampel.flags.PhotoFlags import PhotoFlags
from ampel.base.PlainPhotoPoint import PlainPhotoPoint
from ampel.base.PlainUpperLimit import PlainUpperLimit
from ampel.base.PhotoPoint import PhotoPoint
from ampel.base.UpperLimit import UpperLimit
from ampel.base.LightCurve import LightCurve
from ampel.pipeline.logging.LoggingUtils import LoggingUtils

class LightCurveLoader:
	"""
	Class with methods each returning an instance of ampel.base.LightCurve
	Either through DB query (load_through_db_query) or through parsing of DB query results 
	"""

	_main_col_name = 'main'
	_photo_col_name = 'photo'

	def __init__(self, ampel_db, read_only=True, logger=None):
		"""
		Parameters:
		-----------
		ampel_db: instance of pymongo.database.DataBase 
		read_only: if True, the LightCurve instance returned by the methods of this class will be:
			* a frozen class
			* containing a immutable list (tuple) of PhotoPoint
			* whereby each PhotoPoint is a frozen class as well
			* and each PhotoPoint dict content is an immutable dict
		"""
		self.logger = LoggingUtils.get_logger() if logger is None else logger
		self.main_col = ampel_db[LightCurveLoader._main_col_name] 
		self.photo_col = ampel_db[LightCurveLoader._photo_col_name] 
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
		photo_cursor = self.photo_col.find({"tranId": tran_id})
		main_cursor = self.main_col.find({"_id": compound_id})

		if main_cursor.count() == 0:
			self.logger.warn("No LightCurve found with id: %s " % compound_id)
			return None

		if photo_cursor.count() == 0:
			self.logger.warn("No photopoint found for tranId: %s " % tran_id)
			return None

		self.logger.info(
			"DB query for tranId: %s and compoundId: %s returned %i documents" %
			(tran_id, compound_id, photo_cursor.count())
		)

		pps_list = []
		uls_list = []

		for el in photo_cursor:
			if el['_id'] > 0:
				pps_list.append(el)
			else:
				uls_list.append(el)

		return self.load_using_results(pps_list, uls_list, next(main_cursor))


	def load_using_results(self, ppd_list, uld_list, compound):
		"""
		Creates and returns an instance of ampel.base.LightCurve using db results.
		This function is used at both T2 and T3 levels 

		Required parameters:
		--------------------
		ppd_list: list of photopoint dict instances loaded from DB
		uld_list: list of upper limit dict instances loaded from DB
		compound: compound dict instance loaded from DB or instance of ampel.base.Compound
		"""

		# Robustness check 
		if compound is None:
			raise ValueError("Required parameter 'compound' cannot be None")

		if ppd_list is None or type(ppd_list) is not list:
			raise ValueError("Parameter 'ppd_list' must be a list")

		if uld_list is None:
			uld_list = []

		# List of PhotoPoint object instances
		ppo_list = []

		# List of UpperLimit object instances
		ulo_list = []

		# Loop through compound elements
		for el in compound['comp'] if type(compound) is dict else compound.content:

			# Get corresponding photopoint / upper limit
			if 'pp' in el:

				photo_dict = next((pp for pp in ppd_list if pp["_id"] == el["pp"]), None)

				if photo_dict is None:

					self.logger.warn("Photo point %i not found" % el['pp'])
					self.logger.info("Trying to recover from this error")

					# TODO: populate 'troubles collection'
					cursor = self.photo_col.find({"_id": el['pp']})
	
					if (cursor.count()) == 0:
						self.logger.error("PhotoPoint with id %i not found" % el['pp'])
						raise ValueError("PhotoPoint with id %i not found" % el['pp'])

					photo_dict = next(cursor)

			else:

				photo_dict = next((ul for ul in uld_list if ul["_id"] == el["ul"]), None)	
				if photo_dict is None:
					raise ValueError("Upper limit %i not found" % el['ul'])

			# Create PhotoFlags instance
			flags = FlagUtils.dbflag_to_enumflag(photo_dict['alFlags'], PhotoFlags) 

			# If custom options avail (if dict contains more than the dict key 'pp')
			if (len(el.keys()) > 1):
				
				
				obj = (
					# Create photopoint wrapper instance
					PhotoPoint(photo_dict, flags, read_only=False) if 'pp' in el 
					# Create upperlimit wrapper instance
					else UpperLimit(photo_dict, flags, read_only=False)
				)

				# Update pp options dict and cast internal to immutable dict if required
				obj.set_policy(el, self.read_only)

			# Photopoint defined in the compound has no special policy, i.e len(el.keys()) == 1
			else:
				obj = (
					PlainPhotoPoint(photo_dict, flags, self.read_only) if 'pp' in el 
					else PlainUpperLimit(photo_dict, flags, self.read_only)
				)

			# Update internal list of PhotoPoint/UpperLimit instances
			if 'pp' in el:
				ppo_list.append(obj)
			else:
				ulo_list.append(obj)

		return LightCurve(
			compound['_id'], ppo_list, ulo_list, 
			info={'tier': compound['tier'], 'added': compound['added']}, 
			read_only=self.read_only
		)


	def load_using_objects(self, compound, already_loaded_photo):
		"""
		Creates and returns an instance of ampel.base.LightCurve using db results.
		This function is used at both T2 and T3 levels 

		Required parameters:
		--------------------
		compound: compound dict instance loaded from DB or instance of ampel.base.Compound
		'already_loaded_photo': dict instance containing references to already existing 
		frozen PhotoPoint and UpperLimit instances. PhotoPoint/UpperLimit instances 
		are then 're-used' rather than re-instantiated  for every LightCurve object 
		(different LightCurves can share common Photopoints).
		-> key: photopoint or upperlimit id 
		-> value: corresponding PhotoPoint or UpperLimit instance
		- must only contain PhotoPoint/UpperLimit instances *without* custom policy
		- will only be used if read_only is True (see LightCurveLoader constructor)
		- dict can be populated in case new photopoint/upper limit  instance(s) 
		  is(are) to be loaded by the error recovery procedure (i.e pp id not in dict)
		"""

		# Robustness check 
		if compound is None or already_loaded_photo is None:
			raise ValueError("Invalid parameters")

		# List of PhotoPoint/UpperLimit object instances
		ppo_list = []
		ulo_list = []

		# Loop through compound elements
		for el in compound['comp'] if type(compound) is dict else compound.content:

			# Get corresponding photopoint / upper limit
			if 'pp' in el:

				pp_id = el["pp"]

				if pp_id not in already_loaded_photo:

					self.logger.warn("Photo point %i not found" % pp_id)
					self.logger.info("Trying to recover from this error")

					# TODO: populate 'troubles collection'
					cursor = self.photo_col.find({"_id": pp_id})
	
					if (cursor.count()) == 0:
						self.logger.error("PhotoPoint with id %i not found" % pp_id)
						raise ValueError("PhotoPoint with id %i not found" % pp_id)

					pp_doc = next(cursor)
					pp_flags = FlagUtils.dbflag_to_enumflag(pp_doc['alFlags'], PhotoFlags)

					# Update dict already_loaded_photo
					already_loaded_photo[pp_id] = PlainPhotoPoint(pp_doc, pp_flags, read_only=True)

				# If custom options avail (if dict contains more than the dict key 'pp')
				if (len(el.keys()) > 1):
					ppo_list.append(
						# Create photopoint wrapper instance
						PhotoPoint(
							already_loaded_photo[pp_id].content, 
							already_loaded_photo[pp_id].flags,
							read_only=False
						) 
					)
					ppo_list[-1].set_policy(el, self.read_only)
				else:
					ppo_list.append(already_loaded_photo[pp_id])

			else:

				ul_id = el["ul"]

				if ul_id not in already_loaded_photo:
					raise ValueError("Upper limit %i not found" % ul_id)

				# If custom options avail (if dict contains more than the dict key 'ul')
				if (len(el.keys()) > 1):
					ulo_list.append(
						# Create upperlimit wrapper instance
						UpperLimit(
							already_loaded_photo[ul_id].content, 
							already_loaded_photo[ul_id].flags,
							read_only=False
						) 
					)
					ulo_list[-1].set_policy(el, self.read_only)
				else:
					# Raises Exception if ul_id not found
					ulo_list.append(already_loaded_photo[ul_id])

		return LightCurve(
			compound['_id'] if type(compound) is dict else compound.id, 
			ppo_list, ulo_list, info=(
				{'tier': compound['tier'], 'added': compound['added']} if type(compound) is dict 
				else {'tier': compound.tier, 'added': compound.added}
			), read_only=self.read_only
		)
