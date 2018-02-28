#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/t3/TransientLoader.py
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 13.01.2018
# Last Modified Date: 25.02.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from ampel.base.PhotoPoint import PhotoPoint
from ampel.base.LightCurve import LightCurve
from ampel.base.Transient import Transient
from ampel.base.ScienceRecord import ScienceRecord
from ampel.flags.PhotoPointFlags import PhotoPointFlags
from ampel.flags.TransientFlags import TransientFlags
from ampel.flags.AlDocTypes import AlDocTypes
from ampel.flags.FlagUtils import FlagUtils
from ampel.pipeline.logging.LoggingUtils import LoggingUtils
from ampel.pipeline.common.LightCurveLoader import LightCurveLoader
from ampel.pipeline.common.DBQueryResultOrganizer import DBQueryResultOrganizer
from ampel.pipeline.t3.DBQueryBuilder import DBQueryBuilder

from werkzeug.datastructures import ImmutableList
import operator, logging, json
from enum import Flag
from operator import itemgetter


class TransientLoader:
	"""
	non-working class. Implementation not finished
	"""
	all_doc_types = AlDocTypes.PHOTOPOINT|AlDocTypes.COMPOUND|AlDocTypes.TRANSIENT|AlDocTypes.T2RECORD

	def __init__(self, db, logger=None, collection="main"):

		self.col = db[collection]
		self.logger = LoggingUtils.get_logger() if logger is None else logger
		self.lcl = LightCurveLoader(db, logger=self.logger, collection=collection)
		self.al_pps = {}
		self.lc = {}


	def load_new(
		self, tran_id, content_types=all_doc_types, 
		state="latest", channel_flags=None, t2_ids=None
	):
		"""
		tran_id: transient id (string)

		content_types: AlDocTypes flag combination. Possible values are:

			* 'AlDocTypes.TRANSIENT': 
				-> Add info from DB doc to the returned ampel.base.Transient instance
				-> For example: channels, flags (has processing errors), 
				   latest photopoint observation date, ...

			* 'AlDocTypes.PHOTOPOINT': 
				-> load *all* photopoints avail for this transient (regardless of provided state)
				-> The transient will contain a list of ampel.base.PhotoPoint instances 
				-> No policy is set for all PhotoPoint instances

			* 'AlDocTypes.COMPOUND': 
				-> ampel.base.LightCurve instances are created based on DB documents 
				   (with alDocType AlDocTypes.COMPOUND)
				-> if 'state' is 'latest' or a state id (md5 string) is provided, 
				   only one LightCurve instance is created. 
				   if 'state' is 'all', all available lightcurves are created.
				-> the lightcurve instance(s) will be associated with the 
				   returned ampel.base.Transient instance

			* 'AlDocTypes.T2RECORD': 
				-> ...

		state:
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
				self.col.aggregate(
					DBQueryBuilder.latest_compound_general_query(tran_id)
				)
			)

			self.logger.info(
				" -> Latest lightcurve id: %s " % 
				latest_compound_dict['_id']
			)

			# Build query parameters (will return adequate number of docs)
			search_params = DBQueryBuilder.load_transient_state_query(
				tran_id, 
				content_types, 
				compound_id=latest_compound_dict["_id"], 
				t2_ids=t2_ids,
				comp_already_loaded=True
			)

		# Option 2: Load every available transient state
		elif state == "all":

			# Feedback
			self.logger.info(
				"Retrieving %s for all states of transient %s" % 
				(content_types, tran_id)
			)

			# Build query parameters (will return adequate number of docs)
			search_params = DBQueryBuilder.load_transient_query(
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
			search_params = DBQueryBuilder.load_transient_state_query(
				tran_id, content_types, state, t2_ids=t2_ids
			)
		
		self.logger.debug(
			"Retrieving transient info using query: %s" % 
			search_params
		)

		# Execute DB query
		cursor = self.col.find(search_params)

		# Robustness: check empty
		res_count = cursor.count()
		if res_count == 0:
			self.logger.warn("No db document found associated with %s" % tran_id)
			return None

		# Effectively perform DB query (triggered by casting cursor to list)
		self.logger.info(" -> Fetching %i search results" % res_count)
		res_doc_list = list(cursor)

		# Returns a dict with keys='photopoints', 'compounds', 'transient', 't2records'
		# and values = array of corresponding db dict instances
		grouped_res = DBQueryResultOrganizer.organize(
			res_doc_list,
			photopoints = (AlDocTypes.PHOTOPOINT or AlDocTypes.COMPOUND) in content_types, 
			compounds = AlDocTypes.COMPOUND in content_types, 
			t2records = AlDocTypes.T2RECORD in content_types,
			transient = AlDocTypes.TRANSIENT in content_types
		)

		return self.load_from_results(
			tran_id, 
			photopoints=grouped_res['photopoints'], 
			compounds=grouped_res['compounds'] if state != 'latest' else [latest_compound_dict],
			transient=grouped_res['transient'],
			t2records=grouped_res['t2records'],
			content_types=content_types, 
			state=state
		)


	def load_from_results(
		self, tran_id, photopoints=None, compounds=None, t2records=None, transient=None,
		content_types=all_doc_types, state="latest", tailored_res=False
	):
		"""
			tailored_res: 
				* input (results) docs contain only docs for the given tran_id
				* science_records were only retrieved for the required state
		"""

		# Instanciate ampel.base.Transient object
		al_tran = Transient(tran_id)

		# Instanciate and attach PhotoPoint objects if requested in the content_types
		if AlDocTypes.PHOTOPOINT in content_types:

			# Photopoints instance attached to the transient instance are not bound to a compound 
			# and come thus without policy 
			for pp_dict in photopoints:
				al_tran.add_photopoint(
					PhotoPoint(pp_dict, read_only=True)
				)
					
			# Feedback
			trans_dict = al_tran.get_photopoints()
			self.logger.info(
				" -> %i associated photopoints: %s" % 
				(len(trans_dict), trans_dict.keys())
			)

		# Loading lightcurves was requested (happens based on DB compound documents)
		if AlDocTypes.COMPOUND in content_types:

			# Load all available/multiple compounds for this transient
			if state == "all" or type(state) is list:

				# This dict aims at avoiding unnecesssary re-instanciations 
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
				for comp_dict in compounds:
					al_tran.add_lightcurve(
						self.lcl.load_using_results(
							photopoints, comp_dict, frozen_pps_dict = frozen_pps_dict
						)
					)

				# Find out latest compound/lightcurve
				latest_compound_dict = TransientLoader.get_latest_compound_using_query_results(compounds)
	
				# Feedback
				self.logger.info(" -> latest lightcurve id: %s" % latest_compound_dict['_id'])
				self.logger.info(" -> %i lightcurves loaded" % len(compounds))

				# Creates ref to latest lightcurve in the transient instance
				al_tran.set_latest_lightcurve(
					lightcurve_id=latest_compound_dict['_id']
				)

			else:

				# Load single LightCurve
				al_tran.add_lightcurve(
					self.lcl.load_using_results(
						photopoints, 
						compounds[0], # should be only one
						frozen_pps_dict = (
							al_tran.get_photopoints(copy=False) 
							if AlDocTypes.PHOTOPOINT in content_types 
							else None
						)
					)
				)

				latest_compound_id = compounds[0]['_id']

				if state == "latest":
					al_tran.set_latest_lightcurve(
						lightcurve_id=latest_compound_id
					)

				self.logger.info(
					" -> 1 lightcurve loaded (%s)" % latest_compound_id
				)

		if AlDocTypes.TRANSIENT in content_types:

			al_tran.set_flags(
				TransientFlags(
					FlagUtils.dbflag_to_enumflag(
						transient['alFlags'], TransientFlags
					)
				)
			)

			# Feedback
			self.logger.info(" -> loaded transient info")

			# TODO: do more with tdoc (flags, channels)

		if AlDocTypes.T2RECORD in content_types:

			if state == "all" or type(state) is list or tailored_res is True:
				for t2_doc in t2records:
					al_tran.add_science_record(
						ScienceRecord(t2_doc, read_only=True)
					)
			else:
				comp_id = compounds[0]['_id']
				for t2_doc in t2records:
					if t2_doc['compoundId'] == comp_id:
						al_tran.add_science_record(
							ScienceRecord(t2_doc, read_only=True)
						)

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
	def get_latest_t0_compound_id_from_db(col, tran_id):
		""" 
		Static method. 
		"""
		res = next(
		    col.find(
	       		{
	           		'tranId': tran_id,
	           		'alDocType': AlDocTypes.COMPOUND
	       		},
	       		{
					'tranId': 1,
					'len':1
				}
	    	)
	    	.sort([('len', -1)])
	    	.limit(1),
			None
		)

		return res['_id'] if res is not None else None


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
