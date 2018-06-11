#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/db/DBContentLoader.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 13.01.2018
# Last Modified Date: 11.06.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from bson import ObjectId
import operator, logging, json
from datetime import datetime

from ampel.base.PlainPhotoPoint import PlainPhotoPoint
from ampel.base.UpperLimit import UpperLimit
from ampel.base.PlainUpperLimit import PlainUpperLimit
from ampel.base.Compound import Compound
from ampel.base.LightCurve import LightCurve
from ampel.base.TransientView import TransientView
from ampel.base.ScienceRecord import ScienceRecord
from ampel.flags.TransientFlags import TransientFlags
from ampel.flags.AlDocTypes import AlDocTypes
from ampel.flags.FlagUtils import FlagUtils
from ampel.flags.PhotoFlags import PhotoFlags
from ampel.flags.T2RunStates import T2RunStates
from ampel.pipeline.logging.LoggingUtils import LoggingUtils
from ampel.pipeline.db.LightCurveLoader import LightCurveLoader
from ampel.pipeline.db.DBResultOrganizer import DBResultOrganizer
from ampel.pipeline.db.query.QueryLatestCompound import QueryLatestCompound
from ampel.pipeline.db.query.QueryLoadTransientInfo import QueryLoadTransientInfo
from ampel.pipeline.t3.TransientData import TransientData

class DBContentLoader:
	"""
	"""
	all_doc_types = (
		AlDocTypes.PHOTOPOINT |
		AlDocTypes.UPPERLIMIT |
		AlDocTypes.COMPOUND |
		AlDocTypes.TRANSIENT |
		AlDocTypes.T2RECORD
	)


	def __init__(self, central_db, al_config=None, verbose=False, logger=None):
		"""
		"""
		self.logger = LoggingUtils.get_logger() if logger is None else logger
		TransientData.set_ampel_config(al_config)
		self.lcl = LightCurveLoader(central_db, logger=self.logger)
		self.main_col = central_db["main"]
		self.photo_col = central_db["photo"]
		self.al_pps = {}
		self.lc = {}
		self.verbose = verbose

	# TODO: 
	def load_new(self, tran_id, channels=None, state="latest", content_types=all_doc_types, t2_ids=None):
		"""
		Returns a instance of TransientData

		Arguments:
		----------

		-> tran_id: transient id (string)

		-> channels: string or list of strings

		-> state:
		  * "latest": latest state will be retrieved
		  * "all": all states present in DB (at execution time) will be retrieved
		  * <compound_id> or list of <compound_id>: provided state(s) will be loaded. 
		    The compound id must be either a 32 alphanumerical string or bytes

		-> content_types: AlDocTypes flag combination. 
		Possible values are:
		  * 'AlDocTypes.TRANSIENT': 
			-> Add info from DB doc to the returned TransientData instance
			-> For example: channels, flags (has processing errors), 
			   latest photopoint observation date, ...

		  * 'AlDocTypes.PHOTOPOINT': 
			-> load *all* photopoints avail for this transient (regardless of provided state)
			-> The transient will contain a list of ampel.base.PlainPhotoPoint instances 
			-> No policy is set for all PlainPhotoPoint instances

		  * 'AlDocTypes.UPPERLIMIT': 
			-> load *all* upper limits avail for this transient (regardless of provided state)
			-> The transient will contain a list of ampel.base.PlainUpperLimit instances 
			-> No policy is set for all PlainUpperLimit instances

		  * 'AlDocTypes.COMPOUND': 
			-> ampel.base.LightCurve instances are created based on DB documents 
			   (with alDocType AlDocTypes.COMPOUND)
			-> if 'state' is 'latest' or a state id (md5 string) is provided, 
			   only one LightCurve instance is created. 
			   if 'state' is 'all', all available lightcurves are created.
			-> the lightcurve instance(s) will be associated with the returned TransientData instance

		  * 'AlDocTypes.T2RECORD': 
			...

		-> t2_ids: list of strings
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
				),
				None
			)

			if latest_compound_dict is None:
				self.logger.info("Transient %s not found" % tran_id)
				return None

			self.logger.info(
				" -> Latest lightcurve id: %s " % 
				latest_compound_dict['_id']
			)

			# Build query parameters (will return adequate number of docs)
			search_params = QueryLoadTransientInfo.build_statebound_query(
				tran_id, content_types, latest_compound_dict["_id"], 
				channels, t2_ids, comp_already_loaded=True
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
				tran_id, content_types, channels, t2_ids
			)


		# Option 3: Load a user provided state(s)
		else:

			# Feedback
			self.logger.info(
				"Retrieving %s for state(s) %s of transient %s" % 
				(content_types, state, tran_id)
			)

			# (Lousy/incomplete) check if md5 string was provided
			if type(state) not in (bytes, list):
				raise ValueError("Type of provided state must be bytes or list")

			# Build query parameters (will return adequate number of docs)
			search_params = QueryLoadTransientInfo.build_statebound_query(
				tran_id, content_types, state, channels, t2_ids
			)
		
		self.logger.debug(
			"Retrieving transient info using query: %s" % 
			search_params
		)

		# Execute DB query
		main_cursor = self.main_col.find(search_params)

		# Robustness: check empty
		res_count = main_cursor.count()
		if res_count == 0:
			self.logger.warn("No db document found associated with %s" % tran_id)
			return None

		# Effectively perform DB query (triggered by casting cursor to list)
		self.logger.info(" -> Fetching %i results from main col" % res_count)
		res_main_list = list(main_cursor)

		# Photo DB query 
		if AlDocTypes.PHOTOPOINT|AlDocTypes.UPPERLIMIT in content_types:
			photo_cursor = self.photo_col.find({'tranId': tran_id})
			self.logger.info(" -> Fetching %i photo measurements" % photo_cursor.count())
			res_photo_list = list(photo_cursor)
		else:
			if AlDocTypes.PHOTOPOINT in content_types:
				photo_cursor = self.photo_col.find({'tranId': tran_id, '_id': {'$gt': 0}})
				self.logger.info(" -> Fetching %i photo points" % photo_cursor.count())
				res_photo_list = list(photo_cursor)
			if AlDocTypes.UPPERLIMIT in content_types:
				photo_cursor = self.photo_col.find({'tranId': tran_id, '_id': {'$lt': 0}})
				self.logger.info(" -> Fetching %i upper limits" % photo_cursor.count())
				res_photo_list = list(photo_cursor)

		return self.load_tran_data(
			res_main_list, res_photo_list, channels, state=state
		)


	def load_tran_data(
		self, main_list, photo_list=None, channels=None, state="latest", 
		load_lightcurves=True, feedback=True, verbose_feedback=False
	):
		"""
		"""
		# Convert non array into array for convenience
		if channels is None:
			channels = [None]

		# Build set: we need intersections later
		channels = set(channels)

		# Stores loaded transient items. 
		# Key: tran_id, value: TransientData instance
		register = {}

		# Loop through ampel docs
		tran_id = ""
		for doc in main_list:
			
			# main_list results are sorted by tranId
			if tran_id != doc['tranId']:

				# Instantiate TransientData object
				tran_id = doc['tranId']
				tran_data = TransientData(tran_id, state, self.logger)
				register[tran_id] = tran_data

			# Pick up transient document
			if doc["alDocType"] == AlDocTypes.TRANSIENT:

				# Load, translate alFlags from DB into a TransientFlags enum flag instance 
				# and associate it with the TransientData object instance

				tran_data.set_flags(
					TransientFlags(
						FlagUtils.dbflag_to_enumflag(
							doc['alFlags'], TransientFlags
						)
					)
				)

				for channel in (channels & set(doc['channels'])):

					found_first = False
					last_entry = None

					# Journal entries are time ordered
					for entry in doc['journal']:

						if entry['tier'] != 0 or channel not in entry['chans']:
							continue

						# First entry is creation date 
						if not found_first:
							tran_data.set_created(
								datetime.utcfromtimestamp(entry['dt']), 
								channel
							)
						else:
							last_entry = entry

						if last_entry is not None:
							tran_data.set_modified(
								datetime.utcfromtimestamp(last_entry['dt']), 
								channel
							)

			# Pick compound dicts 
			if doc["alDocType"] == AlDocTypes.COMPOUND:

				tran_data.add_compound(
					Compound(doc, read_only=True), 
					channels if len(channels) == 1 
					else channels & set(doc['channels']) # intersection
				)

				if state == "latest":
					tran_data.set_latest_state(
						doc['_id'], 
						channels if len(channels) == 1 
						else channels & set(doc['channels']) # intersection
					)

			# Pick t2 records
			if doc["alDocType"] == AlDocTypes.T2RECORD:

				sr = ScienceRecord(
					doc['tranId'], doc['t2Unit'], doc['compId'], doc.get('results'),
					info={
						'runConfig': doc['runConfig'], 
						'runState': doc['runState'],
						'genTime': ObjectId(doc['_id']).generation_time,
						'hasError': doc['runState'] == T2RunStates.ERROR
					}, 
					read_only=True
				)

				tran_data.add_science_record(
					sr,
					channels if len(channels) == 1 
					else channels & set(doc['channels']) # intersection
				)


		if photo_list is not None:

			loaded_tran_ids = register.keys()

			# Share common upper limits among different Transients
			loaded_uls = {}

			# Loop through photo results
			for doc in photo_list:
	
				# Pick photo point dicts
				if doc["_id"] > 0:

					
					# Photopoints instance attached to the transient instance 
					# are not bound to a compound and come thus without policy 
					register[doc['tranId']].add_photopoint(
						PlainPhotoPoint(
							doc, flags = FlagUtils.dbflag_to_enumflag(
								doc['alFlags'], PhotoFlags
							),
							read_only=True
						)
					)
	
				# Pick upper limit dicts
				else:
	
					if type(doc['tranId']) is int:
						register[doc['tranId']].add_upperlimit(
							UpperLimit(doc, read_only=True)
						)
					
					else: # list
						
						doc_id = doc["_id"]
						for tran_id in (loaded_tran_ids & doc['tranId']):
							if doc_id not in loaded_uls:
								loaded_uls[doc_id]= PlainUpperLimit(
									doc, FlagUtils.dbflag_to_enumflag(
										doc['alFlags'], PhotoFlags
									),
									read_only=True
								)
							register[tran_id].add_upperlimit(loaded_uls[doc_id])
					 

		if load_lightcurves and photo_list is not None:
		
			for tran_data in register.values():

				if len(tran_data.compounds) == 0:
					continue

				# This dict aims at avoiding unnecesssary re-instantiations 
				# of PhotoPoints objects referenced in several different LightCurves. 
				frozen_photo_dict = {**tran_data.photopoints, **tran_data.upperlimits}
				len_uls = len(tran_data.upperlimits)

				# Transform 
				# {
				#   'HU_LENS': [comp1, comp2, comp3],
				#   'HU_SN': [comp1, comp3]
				# }
				# into:
				# {
				#   'comp1': {'HU_LENS', 'HU_SN'},
				#   'comp2': {'HU_LENS'},
				#   'comp3': {'HU_LENS', 'HU_SN'},
				# }
				inv_map = {}
				for chan_name, comp_list in tran_data.compounds.items():
					for comp_obj in comp_list:
						if comp_obj in inv_map:
							inv_map[comp_obj].append(chan_name)
						else:
							inv_map[comp_obj] = [chan_name]

				for comp, chan_names in inv_map.items():

					if (len_uls == 0 and len([el for el in comp.content if 'ul' in el]) > 0):
						self.logger.info(
							" -> LightCurve loading aborded for %s (upper limits required)" % 
							comp.get_id()
						)
						continue

					lc = self.lcl.load_using_objects(comp, frozen_photo_dict)

					# Associate it to the TransientData instance
					tran_data.add_lightcurve(lc, chan_names)

		# Feedback
		if feedback:
			self.log_feedback(register.values(), photo_list, channels, verbose_feedback)

		return register


	def log_feedback(self, dict_values, photo_list, channels, verbose_feedback):
		"""
		"""


		if channels is None:
			channels = [None]

		for tran_data in dict_values:

			len_comps = len({ell.id for el in tran_data.compounds.values() for ell in el})
			len_lcs = len({ell.id for el in tran_data.lightcurves.values() for ell in el})
			len_srs = len({hash(ell) for el in tran_data.science_records.values() for ell in el})

			if photo_list is not None:

				pps = tran_data.photopoints
				uls = tran_data.upperlimits

				self.logger.info(
					" -> %i loaded: PP: %i, UL: %i, CP: %i, LC: %i, SR: %i" % 
					(tran_data.tran_id, len(pps), len(uls), len_comps, len_lcs, len_srs)
				)

				if verbose_feedback: 

					if len(pps) > 0:
						self.logger.info(
							" -> Photopoint(s): {}".format(
								(*pps,) if len(pps) > 1 else next(iter(pps))
							)
						)

					if len(uls) > 0:
						self.logger.info(
							" -> Upper limit(s): {}".format(
								(*uls,) if len(uls) > 1 else next(iter(uls))
							)
						)
			else:

				self.logger.info(
					" -> %i loaded: PP: 0, UL: 0, CP: %i, LC: %i, SR: %i" % 
					(tran_data.tran_id, len_comps, len_lcs, len_srs)
				)

			if verbose_feedback:
				
				if len_comps > 0:
					for channel in tran_data.compounds.keys():
						self.logger.info(
							" -> %s Compound(s): %s " %
							(
								"" if channel is None else "[%s]" % channel,
								[el.id.hex() for el in tran_data.compounds[channel]]
							)
						)


				if len_lcs > 0:
					for channel in tran_data.lightcurves.keys():
						self.logger.info(
							" -> %s LightCurves(s): %s " %
							(
								"" if channel is None else "[%s]" % channel,
								[el.id.hex() for el in tran_data.lightcurves[channel]]
							)
						)


				if len_srs > 0:

					t2_ids = {ell.t2_unit_id for el in tran_data.science_records.values() for ell in el}
	
					for channel in tran_data.science_records.keys():
						for t2_id in t2_ids:
							srs = len(list(filter(
								lambda x: x.t2_unit_id == t2_id,
								tran_data.science_records[channel]
							)))
							if srs > 0:
								self.logger.info(
									" -> %s T2 %s: %s " %
									(
										"" if channel is None else "[%s]" % channel,
										t2_id, srs	
									)
								)
