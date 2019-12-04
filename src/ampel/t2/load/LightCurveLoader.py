#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/t2/load/LightCurveLoader.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 13.01.2018
# Last Modified Date: 28.11.2019
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Dict, Any, Union, Sequence
from ampel.abstract.AbsT2ObjectLoader import AbsT2ObjectLoader
from ampel.core.PhotoPoint import PhotoPoint
from ampel.core.UpperLimit import UpperLimit
from ampel.object.PlainPhotoPoint import PlainPhotoPoint
from ampel.object.PlainUpperLimit import PlainUpperLimit
from ampel.object.LightCurve import LightCurve
from ampel.logging.AmpelLogger import AmpelLogger
from ampel.db.AmpelDB import AmpelDB


class LightCurveLoader(AbsT2ObjectLoader):
	"""
	Each method returns an instance of :py:class:`LightCurve <ampel.object.LightCurve`.
	Either through DB query (load_through_db_query) or through parsing of DB query results 
	"""

	# pylint: disable=super-init-not-called
	def __init__(self, ampel_db: AmpelDB, read_only=True, logger=None):
		"""
		:param bool read_only: if True, the LightCurve instance returned by the methods of this class will be:
		- a frozen class
		- containing a immutable list (tuple) of PhotoPoint
		- whereby each PhotoPoint is a frozen class as well
		- and each PhotoPoint dict content is an immutable dict
		"""
		self.logger = AmpelLogger.get_logger() if logger is None else logger
		self.read_only = read_only
		self.col_t0 = ampel_db.get_collection("t0")
		self.col_t1 = ampel_db.get_collection("t1")


	def load(self, t2_doc: Dict[str, Any]) -> LightCurve:
		"""
		Load a lightcurve by performing a DB query

		:param dict t2_doc: t2 document loaded from DB
		"""
		return self.load_from_db(
			t2_doc['stockId'], 
			t2_doc['docId']
		)


	def load_from_db(self, tran_id: Union[int, str], compound_id) -> LightCurve:
		"""
		Load a lightcurve by performing a DB query and feeding the results 
		to the method 'load_from_db_results' from this class.
		This function returns an instance of ampel.object.LightCurve

		:param int tran_id: transient id (int or string)
		:param compound_id: instance of :py:class:`Binary <bson.binary.Binary>` (subtype 5)
		"""

		# TODO : provide list or cursor as func parameter ?
		# T3 will have larger queries (including t3 results)
		cursor_t0 = self.col_t0.find({"stockId": tran_id})

		# pymongo 'sequence' are always list
		if isinstance(compound_id, list):
			match_crit = {"_id": {'$in': compound_id}}
		else:
			match_crit = {"_id": compound_id}

		# Retrieve compound document
		cursor_t1 = self.col_t1.find(match_crit)

		if cursor_t1.count() == 0:
			self.logger.warn(
				"No compound found with the given doc id", 
				extra={
					'stockId': tran_id, 
					'docId': compound_id
				}
			)
			return None

		if cursor_t0.count() == 0:
			self.logger.warn("No t0 data found", extra={'stockId': tran_id})
			return None

		self.logger.debug(
			None, {
				'stockId': tran_id,
				'docId': compound_id,
				'nDoc': cursor_t0.count()
			}
		)

		pps_list = []
		uls_list = []

		for el in cursor_t0:
			if el['_id'] > 0:
				pps_list.append(el)
			else:
				uls_list.append(el)

		return self.load_using_results(
			pps_list, uls_list, next(cursor_t1)
		)


	def load_using_results(
		self, ppd_list: Sequence[Dict], uld_list: Sequence[Dict], compound
	) -> LightCurve:
		"""
		Creates and returns an instance of ampel.object.LightCurve using db results.
		This function is used at both T2 and T3 levels 

		:param ppd_list: list of photopoint dict instances loaded from DB
		:param uld_list: list of upper limit dict instances loaded from DB
		:param compound: compound doc (dict instance) loaded from DB
		"""

		# Robustness check 
		if compound is None:
			raise ValueError("Required parameter 'compound' cannot be None")

		if ppd_list is None or not isinstance(ppd_list, list):
			raise ValueError("Parameter 'ppd_list' must be a list")

		if uld_list is None:
			uld_list = []

		# List of PhotoPoint object instances
		ppo_list = []

		# List of UpperLimit object instances
		ulo_list = []

		# Loop through compound elements
		for el in compound['comp']:

			# Check exclusion
			if 'excl' in el:
				self.logger.debug(
					"Ignoring excluded photodata",
					extra={
						'pp': el['pp'] if 'pp' in el else el['ul'],
						'reason': el['excl']
					}
				)
				continue

			# Get corresponding photopoint / upper limit
			if 'pp' in el:

				photo_dict = next(
					(pp for pp in ppd_list if pp["_id"] == el["pp"]), 
					None
				)

				if photo_dict is None:

					self.logger.warn(
						"Photo point not found, trying to recover",
						extra={'pp': el['pp']}
					)

					# TODO: populate 'troubles collection'
					cursor = self.col_t0.find({"_id": el['pp']})
	
					if (cursor.count()) == 0:
						self.logger.error("PhotoPoint not found", extra={'pp': el['pp']})
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
				obj = (
					PlainPhotoPoint(photo_dict, self.read_only) if 'pp' in el 
					else PlainUpperLimit(photo_dict, self.read_only)
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


	def load_using_objects(self, compound, already_loaded_photo) -> LightCurve:
		"""
		Creates and returns an instance of ampel.object.LightCurve using db results.
		This function is used at both T2 and T3 levels 

		:param compound: namedtuple loaded from DB
		:param dict already_loaded_photo: dict instance containing references to already existing 
		frozen PhotoPoint and UpperLimit instances. PhotoPoint/UpperLimit instances 
		are then 're-used' rather than re-instantiated  for every LightCurve object 
		(different LightCurves can share common Photopoints).
		- key: photopoint or upperlimit id 
		- value: corresponding PhotoPoint or UpperLimit instance
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
		for el in compound.comp:

			# Check exclusion
			if 'excl' in el:
				self.logger.debug(
					"Ignoring excluded photodata",
					extra={
						'pp': el['pp'] if 'pp' in el else el['ul'],
						'reason': el['excl']
					}
				)
				continue

			# Get corresponding photopoint / upper limit
			if 'pp' in el:

				# Shortcut
				pp_id = el["pp"]

				if pp_id not in already_loaded_photo:

					self.logger.warn(
						"Photo point not provided, trying to recover",
						extra={'pp': pp_id}
					)

					cursor = self.col_t0.find({"_id": pp_id})
	
					if (cursor.count()) == 0:
						# TODO: populate 'troubles collection'
						self.logger.error("PhotoPoint not found", extra={'pp': pp_id})
						raise ValueError("PhotoPoint with id %i not found" % pp_id)

					pp_doc = next(cursor)

					# Update dict already_loaded_photo
					already_loaded_photo[pp_id] = PlainPhotoPoint(pp_doc, read_only=True)


				# If custom options avail (if dict contains more than the dict key 'pp')
				if (len(el.keys()) > 1):
					ppo_list.append(
						# Create photopoint wrapper instance
						PhotoPoint(
							already_loaded_photo[pp_id].content, 
							already_loaded_photo[pp_id].flag,
							read_only=False
						) 
					)
					ppo_list[-1].set_policy(el, self.read_only)
				else:
					ppo_list.append(already_loaded_photo[pp_id])

			else:

				# Shortcut
				ul_id = el["ul"]

				if ul_id not in already_loaded_photo:
					raise ValueError("Upper limit %i not found" % ul_id)

				# If custom options avail (if dict contains more than the dict key 'ul')
				if (len(el.keys()) > 1):
					ulo_list.append(
						# Create upperlimit wrapper instance
						UpperLimit(
							already_loaded_photo[ul_id].content, 
							already_loaded_photo[ul_id].flag,
							read_only=False
						) 
					)
					ulo_list[-1].set_policy(el, self.read_only)
				else:
					# Raises Exception if ul_id not found
					ulo_list.append(already_loaded_photo[ul_id])

		return LightCurve(
			compound.id, ppo_list, ulo_list, read_only=self.read_only,
			info={'tier': compound.tier, 'added': compound.added} 
		)
