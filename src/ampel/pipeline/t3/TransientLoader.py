#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/t3/TransientLoader.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 13.01.2018
# Last Modified Date: 29.05.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from ampel.base.PhotoPoint import PhotoPoint
from ampel.base.UpperLimit import UpperLimit
from ampel.base.LightCurve import LightCurve
from ampel.base.Transient import Transient
from ampel.base.ScienceRecord import ScienceRecord
from ampel.flags.TransientFlags import TransientFlags
from ampel.flags.AlDocTypes import AlDocTypes
from ampel.flags.FlagUtils import FlagUtils
from ampel.pipeline.logging.LoggingUtils import LoggingUtils
from ampel.pipeline.db.LightCurveLoader import LightCurveLoader
from ampel.pipeline.db.DBResultOrganizer import DBResultOrganizer
from ampel.pipeline.db.query.QueryLatestCompound import QueryLatestCompound
from ampel.pipeline.db.query.QueryLoadTransientInfo import QueryLoadTransientInfo

import operator, logging, json
from operator import itemgetter
from datetime import datetime


class TransientLoader:
	"""
	"""
	all_doc_types = (
		AlDocTypes.PHOTOPOINT |
		AlDocTypes.UPPERLIMIT |
		AlDocTypes.COMPOUND |
		AlDocTypes.TRANSIENT |
		AlDocTypes.T2RECORD
	)


	# TODO: implement include logs
	def __init__(self, db, logger=None, save_channels=False, include_logs=False):
		"""
		"""

		self.main_col = db["main"]
		self.photo_col = db["photo"]
		self.logger = LoggingUtils.get_logger() if logger is None else logger
		self.lcl = LightCurveLoader(db, logger=self.logger)
		self.al_pps = {}
		self.lc = {}
		self.save_channels = save_channels


	def load_new(
		self, tran_id, content_types=all_doc_types, 
		state="latest", channel_flags=None, t2_ids=None
	):
		"""
		Arguments:
		----------

		-> tran_id: transient id (string)

		-> content_types: AlDocTypes flag combination. 
		Possible values are:
		* 'AlDocTypes.TRANSIENT': 
			-> Add info from DB doc to the returned ampel.base.Transient instance
			-> For example: channels, flags (has processing errors), 
			   latest photopoint observation date, ...

		* 'AlDocTypes.PHOTOPOINT': 
			-> load *all* photopoints avail for this transient (regardless of provided state)
			-> The transient will contain a list of ampel.base.PhotoPoint instances 
			-> No policy is set for all PhotoPoint instances

		* 'AlDocTypes.UPPERLIMIT': 
			-> load *all* upper limits avail for this transient (regardless of provided state)
			-> The transient will contain a list of ampel.base.UpperLimit instances 
			-> No policy is set for all UpperLimit instances

		* 'AlDocTypes.COMPOUND': 
			-> ampel.base.LightCurve instances are created based on DB documents 
			   (with alDocType AlDocTypes.COMPOUND)
			-> if 'state' is 'latest' or a state id (md5 string) is provided, 
			   only one LightCurve instance is created. 
			   if 'state' is 'all', all available lightcurves are created.
			-> the lightcurve instance(s) will be associated with the 
			   returned ampel.base.Transient instance

		* 'AlDocTypes.T2RECORD': 
			...

		-> state:
		* "latest": latest state will be retrieved
		* "all": all states present in DB (at execution time) will be retrieved
		* <compound_id>: provided state will be loaded. 
		  The compound id must be a 32 alphanumerical string
	
		"""

		# Robustness check 1
		if content_types is None or content_types == 0:
			raise ValueError("Parameter content_types not conform")

		# Option 1: Find latest state, then update search query parameters
		if state == "latest":

			# Feedback
			self.logger.info(
				"Retrieving %s for latest state of transient %s" % 
				(content_types, tran_id)
			)

			# Execute DB query returning a dict represenation 
			# of the latest compound dict (alDocType: COMPOUND) 
			latest_compound_dict = next(
				self.main_col.aggregate(
					QueryLatestCompound.general_query(tran_id)
				)
			)

			self.logger.info(
				" -> Latest lightcurve id: %s " % 
				latest_compound_dict['_id']
			)

			# Build query parameters (will return adequate number of docs)
			search_params = QueryLoadTransientInfo.build_statebound_query(
				tran_id, 
				content_types, 
				compound_ids = latest_compound_dict["_id"], 
				t2_ids = t2_ids,
				comp_already_loaded = True
			)

		# Option 2: Load every available transient state
		elif state == "all":

			# Feedback
			self.logger.info(
				"Retrieving %s for all states of transient %s" % 
				(content_types, tran_id)
			)

			# Build query parameters (will return adequate number of docs)
			search_params = QueryLoadTransientInfo.build_stateless_query(
				tran_id, content_types, t2_ids=t2_ids
			)


		# Option 3: Load a user provided state(s)
		else:

			# Feedback
			self.logger.info(
				"Retrieving %s for state %s of transient %s" % 
				(content_types, state, tran_id)
			)

			# (Lousy/incomplete) check if md5 string was provided
			if len(state) != 32 and type(state) is not list:
				raise ValueError("Provided state must be 32 alphanumerical characters or a list")

			# Build query parameters (will return adequate number of docs)
			search_params = QueryLoadTransientInfo.build_statebound_query(
				tran_id, content_types, state, t2_ids = t2_ids
			)
		
		self.logger.debug(
			"Retrieving transient info using query: %s" % 
			search_params
		)

		# Execute DB query
		cursor = self.main_col.find(search_params)

		# Robustness: check empty
		res_count = cursor.count()
		if res_count == 0:
			self.logger.warn("No db document found associated with %s" % tran_id)
			return None

		# Effectively perform DB query (triggered by casting cursor to list)
		self.logger.info(" -> Fetching %i search results" % res_count)
		res_doc_list = list(cursor)

		# Photo DB query 
		if AlDocTypes.PHOTOPOINT|AlDocTypes.UPPERLIMIT in content_types:
			photo_cursor = self.main_col.find({'tranId': tran_id})
			self.logger.info(" -> Fetching %i photo measurements" % photo_cursor.count())
			res_doc_list += list(photo_cursor)
		elif AlDocTypes.PHOTOPOINT in content_types:
			photo_cursor = self.main_col.find({'tranId': tran_id, '_id': {'$gt': 0}})
			self.logger.info(" -> Fetching %i photo points" % photo_cursor.count())
			res_doc_list += list(photo_cursor)
		elif AlDocTypes.UPPERLIMIT in content_types:
			photo_cursor = self.main_col.find({'tranId': tran_id, '_id': {'$lt': 0}})
			self.logger.info(" -> Fetching %i upper limits" % photo_cursor.count())
			res_doc_list += list(photo_cursor)

		# Returns a dict with keys = 'photopoints', 'upperlimits', 'compounds', 
		# 'transient', 't2records' and values = array of corresponding db dict instances
		grouped_res = DBResultOrganizer.organize(
			res_doc_list,
			photopoints = (
				AlDocTypes.PHOTOPOINT in content_types or 
				AlDocTypes.COMPOUND in content_types
			), 
			upperlimits = (
				AlDocTypes.UPPERLIMIT in content_types or 
				AlDocTypes.COMPOUND in content_types
			), 
			compounds = AlDocTypes.COMPOUND in content_types, 
			t2records = AlDocTypes.T2RECORD in content_types,
			transient = AlDocTypes.TRANSIENT in content_types
		)

		return self.load_from_results(
			tran_id, 
			pp_docs = grouped_res['photopoints'], 
			ul_docs = grouped_res['upperlimits'], 
			compound_docs = grouped_res['compounds'] if state != 'latest' else [latest_compound_dict],
			tran_doc = grouped_res['transient'],
			t2_docs = grouped_res['t2records'],
			content_types = content_types, 
			state = state
		)


	def load_from_results(
		self, tran_id, pp_docs=None, ul_docs=None, compound_docs=None, t2_docs=None, tran_doc=None,
		state="latest", content_types=all_doc_types, tailored_res=False
	):
		"""
		tailored_res: 
			* input (results) docs contain only docs for the given tran_id
			* science_records were only retrieved for the required state
		"""

		# Instantiate ampel.base.Transient object
		al_tran = Transient(tran_id)

		if self.save_channels:
			channel_register = al_tran.new_channel_register()

		# Instantiate and attach PhotoPoint objects if requested in the content_types
		if AlDocTypes.PHOTOPOINT in content_types and pp_docs is not None:

			# Photopoints instance attached to the transient instance are not bound to a compound 
			# and come thus without policy 
			for pp_dict in pp_docs:
				al_tran.add_photopoint(
					PhotoPoint(pp_dict, read_only=True)
				)
					
			# Feedback
			pps = al_tran.get_photopoints()
			self.logger.info(
				" -> {} associated photopoint(s): {}".format(
					len(pps), (*pps,) if len(pps) > 1 else next(iter(pps.keys()))
				)
			)

		# Instantiate and attach UpperLimit objects if requested in the content_types
		if AlDocTypes.UPPERLIMIT in content_types and ul_docs is not None:

			# UpperLimits instance attached to the transient instance 
			# are not bound to a compound and come thus without policy 
			for ul_dict in ul_docs:
				al_tran.add_upperlimit(
					UpperLimit(ul_dict, read_only=True)
				)
					
			# Feedback
			uls = al_tran.get_upperlimits()
			self.logger.info(
				" -> {} associated upper limit(s): {}".format(
					len(uls), (*uls,) if len(uls) > 1 else next(iter(uls.keys()))
				)
			)

		# Loading lightcurves was requested 
		# (loading is made based on DB 'compound' documents)
		if AlDocTypes.COMPOUND in content_types and compound_docs is not None:

			# Load all available/multiple compounds for this transient
			if state == "all" or type(state) is list:

				# This dict aims at avoiding unnecesssary re-instantiations 
				# of PhotoPoints objects referenced in several different LightCurves. 
				# TODO: rephrase / implement better, shorter description
				# Note that in the following rare case that:
				# 	-> the provided content_types include photopoints
				# 	-> and some photopoint ids referenced in compounds are not associated with this transient
				#     (happens if IPAC matching radius is too large for example)
				# then, new photopoints will be added to the internal photopoint dict of the transient instance
				# since frozen_pps_dict is just a reference to this internal dict
				frozen_pps_dict = (
					al_tran.get_photopoints(copy=False) 
					if AlDocTypes.PHOTOPOINT in content_types 
					else {}
				)
			
				# Loop through all compounds
				for comp_dict in compound_docs:

					# Intanciate ampel.base.LightCurve object
					lc = self.lcl.load_using_results(
						pp_docs, ul_docs, comp_dict, frozen_pps_dict = frozen_pps_dict
					)

					# Associate it to the ampel.base.Transient instance
					al_tran.add_lightcurve(lc)

					# Save channel associations if so wished
					if self.save_channels:
						channel_register.add_lightcurve(comp_dict['channels'], lc)

				# Find out latest compound/lightcurve
				latest_compound_dict = TransientLoader.get_latest_compound_using_query_results(compound_docs)
	
				# Feedback
				self.logger.info(" -> latest lightcurve id: %s" % latest_compound_dict['_id'])
				self.logger.info(" -> %i lightcurves loaded" % len(compound_docs))

				# Creates ref to latest lightcurve in the transient instance
				al_tran.set_latest_lightcurve(
					lightcurve_id=latest_compound_dict['_id']
				)

			# Load a single compound (state is not 'all' and not a list)
			# state can be 'latest' or a specified state
			else:

				# Intanciate ampel.base.LightCurve object
				lc = self.lcl.load_using_results(
					pp_docs, ul_docs, compound_docs[0], # should be only one
					frozen_pps_dict = (
						al_tran.get_photopoints(copy=False) 
						if AlDocTypes.PHOTOPOINT in content_types 
						else None
					)
				)

				# Associate it to the ampel.base.Transient instance
				al_tran.add_lightcurve(lc)

				# Save channel associations if so wished
				if self.save_channels:
					channel_register.add_lightcurve(compound_docs[0]['channels'], lc)

				# single compound (state is not 'all' and not a list)
				latest_compound_id = compound_docs[0]['_id']

				# If state was defined as being the latest, 
				# then save this info into the transient instance
				if state == "latest":
					al_tran.set_latest_lightcurve(
						lightcurve_id=latest_compound_id
					)

				# Feedback
				self.logger.info(
					" -> 1 lightcurve loaded (%s)" % latest_compound_id
				)

		if AlDocTypes.TRANSIENT in content_types and tran_doc is not None:

			# Load, translate alFlags from DB into a TransientFlags enum flag instance 
			# and associate it with the ampel.base.Transient object instance
			al_tran.set_parameter(
				"flags",
				TransientFlags(
					FlagUtils.dbflag_to_enumflag(
						tran_doc['alFlags'], TransientFlags
					)
				)
			)

			# Load transient doc creation date from ObjectId
			al_tran.set_parameter(
				"created",
				tran_doc['_id'].generation_time
			)

			# Load transient modified time as datetime 
			al_tran.set_parameter(
				"modified",
				datetime.utcfromtimestamp(
					tran_doc['modified']
				)
			)
			
			# Feedback
			self.logger.info(" -> loaded transient info")

		if AlDocTypes.T2RECORD in content_types and t2_docs is not None:

			if state == "all" or type(state) is list or tailored_res is True:

				for t2_doc in t2_docs:

					sr = ScienceRecord(t2_doc, read_only=True)
					al_tran.add_science_record(sr)

					if self.save_channels:
						channel_register.add_science_record(t2_doc['channels'], sr)

			else:

				comp_id = compound_docs[0]['_id']

				for t2_doc in t2_docs:

					if t2_doc['compoundId'] == comp_id:

						sr = ScienceRecord(t2_doc, read_only=True)
						al_tran.add_science_record(sr)

						if self.save_channels:
							channel_register.add_science_record(t2_doc['channels'], sr)

			self.logger.info(
				" -> %i science records loaded" % 
				len(al_tran.get_science_records(flatten=True))
			)

		return al_tran


	def load_many(
		self, tran_ids, content_types=all_doc_types, t2_ids=None
	):

		pass	


	@staticmethod
	def get_latest_compound_using_query_results(compounds):
		""" 
		Static method. 

		From a list of compound dict instances, return the compound dict 
		that corresponds to the latest state of the transient.
		Finding this out is not as obvious as it may appear at first.

		The same selection algorithm is also implemented in the method:
			DBQueryBuilder.latest_compound_query(tran_id) 
		which is evaluated by the mongoDB database aggregation framework
		(instead of locally by Python based on DB query results 
		as is the case for the present method).
		"""

		# Check exit
		if len(compounds) == 1:
			return compounds[0]


		# 1) sort compounds by added date
		#################################

		date_added_sorted_comps = sorted(
			compounds, key=itemgetter('added'), reverse=True
		)


		# 2) group first elements with same source (src) 
		# and consider only the first group
		################################################

		ref_tier = date_added_sorted_comps[0]["tier"]
		first_group_comps = []
		for comp in date_added_sorted_comps:
			if comp["tier"] == ref_tier:
				first_group_comps.append(comp)
			else:
				break
		
		# Check exit
		if len(first_group_comps) == 1:
			return first_group_comps[0]


		# 3) Implement tier based sort strategy
		#######################################
		
		# T0: return compound with latest pp date
		if ref_tier == 0:

			lastppdt_sorted_lcs = sorted(
				first_group_comps, key=itemgetter('len'), reverse=True
			)

			return lastppdt_sorted_lcs[0]

		# T1 or T3: return first element (newest added date)
		elif ref_tier == 1 or ref_tier == 3:
			return first_group_comps[0]
	
		else:
			raise NotImplementedError(
				"Sort algorithm not implemented for tier %i" % ref_tier
			)
