#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/db/DBContentLoader.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 13.01.2018
# Last Modified Date: 23.08.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from bson import ObjectId
from datetime import datetime
from collections import namedtuple

from ampel.base.LightCurve import LightCurve
from ampel.base.TransientView import TransientView
from ampel.base.ScienceRecord import ScienceRecord
from ampel.base.PlainPhotoPoint import PlainPhotoPoint
from ampel.base.PlainUpperLimit import PlainUpperLimit
from ampel.base.Compound import Compound

from ampel.core.flags.FlagUtils import FlagUtils
from ampel.core.flags.AlDocTypes import AlDocTypes
from ampel.core.flags.T2RunStates import T2RunStates
from ampel.core.flags.CompoundFlags import CompoundFlags
from ampel.base.flags.TransientFlags import TransientFlags
from ampel.base.flags.PhotoFlags import PhotoFlags

from ampel.pipeline.common.AmpelUtils import AmpelUtils
from ampel.pipeline.t3.TransientData import TransientData
from ampel.pipeline.logging.LoggingUtils import LoggingUtils
from ampel.pipeline.db.AmpelDB import AmpelDB
from ampel.pipeline.db.LightCurveLoader import LightCurveLoader
from ampel.pipeline.db.query.QueryLatestCompound import QueryLatestCompound
from ampel.pipeline.db.query.QueryLoadTransientInfo import QueryLoadTransientInfo

from ampel.base.Frozen import Frozen

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


	def __init__(self, central_db=None, verbose=False, logger=None):
		"""
		:param central_db: string. Use provided DB name rather than Ampel default database ('Ampel')
		"""
		self.logger = LoggingUtils.get_logger() if logger is None else logger
		self.lcl = LightCurveLoader(central_db, logger=self.logger)

		# Optional override of AmpelConfig defaults
		if central_db is None:
			self.main_col = AmpelDB.get_collection("main")
			self.photo_col = AmpelDB.get_collection("photo")
		else:
			self.main_col = central_db["main"]
			self.photo_col = central_db["photo"]

		self.al_pps = {}
		self.lc = {}
		self.verbose = verbose


	def load_new(
		self, tran_id, channels=None, state_op="$latest", states=None, 
		content_types=all_doc_types, t2_ids=None, verbose=True, debug=False
	):
		"""
		Returns a instance of TransientData

		Arguments:
		----------

		:param tran_id: transient id (string). 
		Can be a multiple IDs (list) if state_op is '$all' or states are provided (states cannot be '$latest')

		:param channels: string or list of strings

		:param state_op:
		  * "$latest": latest state will be retrieved
		  * "$all": all states present in DB (at execution time) will be retrieved

		:param states:
		  * <compound_id> or list of <compound_id>: provided state(s) will be loaded. 
		    Each compound id must be either:
				- a 32 alphanumerical string
				- 16 bytes (128 bits)
				- a bson.binary.Binary instance with subtype 5

		:param content_types: AlDocTypes flags. Possible values are:

		  * 'AlDocTypes.TRANSIENT': 
			-> Add info from DB doc to the returned TransientData instance
			-> For example: channels, flags (has processing errors), 
			   latest photopoint observation date, ...

		  * 'AlDocTypes.PHOTOPOINT': 
			-> load *all* photopoints avail for this transient (regardless of provided state)
			-> The transient will contain a list of ampel.base.PlainPhotoPoint instances 
			-> (PlainPhotoPoint (unlike PhotoPoint) instances come without policy)

		  * 'AlDocTypes.UPPERLIMIT': 
			-> load *all* upper limits avail for this transient (regardless of provided state)
			-> The transient will contain a list of ampel.base.PlainUpperLimit instances 
			-> (PlainUpperLimit (unlike UpperLimit) instances come without policy)

		  * 'AlDocTypes.COMPOUND': 
			-> ampel.base.LightCurve instances are created based on DB documents 
			   (with alDocType AlDocTypes.COMPOUND)
			-> if 'state' is '$latest' or a state id (md5 bytes) is provided, 
			   only one LightCurve instance is created. 
			   if 'state' is '$all', all available lightcurves are created.
			-> the lightcurve instance(s) will be associated with the returned TransientData instance

		  * 'AlDocTypes.T2RECORD': 
			...

		:param t2_ids: list of strings
		"""

		# Robustness check 1
		if content_types is None or content_types == 0:
			raise ValueError("Parameter content_types not conform")


		# Option 2: Load a user provided state(s)
		if states is not None:

			# Feedback
			self.logger.info(
				"Retrieving %s for provided state(s) of transient(s) %s" % 
				(content_types, tran_id)
			)

			# Build query parameters (will return adequate number of docs)
			search_params = QueryLoadTransientInfo.build_statebound_query(
				tran_id, content_types, states, channels, t2_ids
			)

		else:
		
			# Option 2: Find latest state, then update search query parameters
			if state_op == "$latest":
	
				if type(tran_id) in (list, tuple):
					raise ValueError("Querying multiple transient ids not supported with state_op == '$latest'")
	
				# Feedback
				self.logger.info(
					"Retrieving %s for latest state of transient %s" % 
					(content_types, tran_id)
				)
	
				# Execute DB query returning the latest compound dict
				latest_compound_dict = next(
					self.main_col.aggregate(
						QueryLatestCompound.general_query(tran_id)
					), None
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
	
			# Option 3: Load every available transient state
			elif state_op == "$all":
	
				# Feedback
				self.logger.info(
					"Retrieving %s for all states of transient(s) %s" % 
					(content_types, tran_id)
				)
	
				# Build query parameters (will return adequate number of docs)
				search_params = QueryLoadTransientInfo.build_stateless_query(
					tran_id, content_types, channels, t2_ids
				)


		self.logger.debug(
			"Retrieving transient(s) info using query: %s" % 
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
		res_photo_list = None

		# Photo DB query 
		if (AlDocTypes.PHOTOPOINT|AlDocTypes.UPPERLIMIT) in content_types:

			photo_cursor = self.photo_col.find(
				{'tranId': {'$in': tran_id} if type(tran_id) in (list, tuple) else tran_id}
			)
			self.logger.info(" -> Fetching %i photo measurements" % photo_cursor.count())
			res_photo_list = list(photo_cursor)

		else:

			if AlDocTypes.PHOTOPOINT in content_types:
				photo_cursor = self.photo_col.find(
					{
						'tranId': {'$in': tran_id} if type(tran_id) in (list, tuple) else tran_id, 
						'_id': {'$gt': 0}
					}
				)

				self.logger.info(" -> Fetching %i photo points" % photo_cursor.count())
				res_photo_list = list(photo_cursor)

			if AlDocTypes.UPPERLIMIT in content_types:
				photo_cursor = self.photo_col.find(
					{
						'tranId': {'$in': tran_id} if type(tran_id) in (list, tuple) else tran_id, 
						'_id': {'$lt': 0}
					}
				)
				self.logger.info(" -> Fetching %i upper limits" % photo_cursor.count())
				res_photo_list = list(photo_cursor)


		return self.create_tran_data(
			res_main_list, res_photo_list, channels, state_op,
			verbose=verbose, debug=debug
		)


	def create_tran_data(
		self, main_list, photo_list=None, channels=None, state_op=None,
		load_lightcurves=True, verbose=True, debug=False
	):
		"""
		"""
		# Build set: we need intersections later
		channels_set = AmpelUtils.to_set(channels)

		# Stores loaded transient items. 
		# Key: tran_id, value: TransientData instance
		tran_register = {}

		# Loop through ampel docs
		tran_id = ""
		for doc in main_list:
			
			tran_id = doc['tranId']

			if tran_id in tran_register:
				tran_data = tran_register[tran_id]
			else:
				tran_data = TransientData(tran_id, state_op, self.logger)
				tran_register[tran_id] = tran_data

			# Pick up transient document
			if doc["alDocType"] == AlDocTypes.TRANSIENT:

				# Load, translate alFlags from DB into a TransientFlags enum flag instance 
				# and associate it with the TransientData object instance
				tran_data.set_flags(
					FlagUtils.dbflag_to_enumflag(
						doc['alFlags'], TransientFlags
					)
				)

				# Use all avail channels if no channel query constraint was used, 
				# otherwise: intersection
				tran_data.set_channels(
					# Transient doc['channels'] cannot be None
					AmpelUtils.to_set(doc['channels']) if channels is None
					else (channels_set & set(doc['channels'])) # intersection
				)

				# Save journal entries related to provided channels
				for entry in doc['journal']:

					# Not sure if those entries will exist. Time will tell.
					if entry.get('channel(s)') is None:
						self.logger.warn(
							'Ignoring following channel-less journal entry: %s' % 
							str(entry)
						)
						continue

					# Set intersection between registered and requested channels (if any)
					chans_intersec = (
						entry['channel(s)'] if channels is None 
						else (channels_set & AmpelUtils.to_set(entry['channel(s)']))
					)

					# Removing embedded 'channels' key/value and add journal entry 
					# to transient data while maintaining the channel(s) association
					tran_data.add_journal_entry(
						chans_intersec,
						# journal entry without the channels key/value
						{k:v for k, v in entry.items() if k != 'channel(s)'}	
					)


			# Pick compound dicts 
			if doc["alDocType"] == AlDocTypes.COMPOUND:

				doc['id'] = doc.pop('_id')
				doc_chans = doc.pop('channels')
				doc['alFlags'] = FlagUtils.dbflag_to_enumflag(
					doc['alFlags'], CompoundFlags
				)

				comp_chans = doc_chans if channels is None else (channels_set & set(doc_chans))

				tran_data.add_compound(
					comp_chans, # intersection
					Compound(**AmpelUtils.recursive_freeze(doc)),
				)

				if len(comp_chans) == 1 and state_op == "$latest":
					tran_data.set_latest_state(comp_chans, doc['id'])

			# Pick t2 records
			if doc["alDocType"] == AlDocTypes.T2RECORD:

				science_record = ScienceRecord(
					doc['tranId'], doc['t2Unit'], doc['compId'], doc.get('results'),
					info={
						'runConfig': doc['runConfig'], 
						'runState': doc['runState'],
						'created': ObjectId(doc['_id']).generation_time,
						'hasError': doc['runState'] == T2RunStates.ERROR
					}
				)

				tran_data.add_science_record(
					# ScienceRecord doc['channels'] cannot be None
					doc['channels'] if channels is None 
					else (channels_set & set(doc['channels'])), # intersection
					science_record
				)


		if photo_list is not None:

			loaded_tran_ids = tran_register.keys()

			# Share common upper limits among different Transients
			loaded_uls = {}

			# Loop through photo results
			for doc in photo_list:

				# Generate PhotoFlags
				photo_flag = FlagUtils.dbflag_to_enumflag(
					doc['alFlags'], PhotoFlags
				)

				# Pick photo point dicts
				if doc["_id"] > 0:
					
					# Photopoints instance attached to the transient instance 
					# are not bound to a compound and come thus without policy 
					tran_register[doc['tranId']].add_photopoint(
						PlainPhotoPoint(doc, photo_flag, read_only=True)
					)
	
				# Pick upper limit dicts
				else:

					# UpperLimits instance attached to the transient instance 
					# are not bound to a compound and come thus without policy 
					if type(doc['tranId']) is int:
						tran_register[doc['tranId']].add_upperlimit(
							PlainUpperLimit(doc, photo_flag, read_only=True)
						)
					
					else: # list
						
						doc_id = doc["_id"]
						for tran_id in (loaded_tran_ids & doc['tranId']):
							if doc_id not in loaded_uls:
								loaded_uls[doc_id]= PlainUpperLimit(
									doc, photo_flag, read_only=True
								)
							tran_register[tran_id].add_upperlimit(loaded_uls[doc_id])
					 

		if load_lightcurves and photo_list is not None:
		
			for tran_data in tran_register.values():

				if len(tran_data.compounds) == 0:
					continue

				# This dict aims at avoiding unnecesssary re-instantiations 
				# of PhotoPoints objects referenced in several different LightCurves. 
				frozen_photo_dicts = {**tran_data.photopoints, **tran_data.upperlimits}
				len_uls = len(tran_data.upperlimits)

				# Transform 
				# {
				#   'HU_LENS': [comp1, comp2, comp3],
				#   'HU_SN': [comp1, comp3]
				# }
				# into:
				# { 'comp_id1': comp1,
				#   'comp_id2': comp2,
				#   'comp_id2': comp4,
				# }
				# and
				# {
				#   'comp_id1': {'HU_LENS', 'HU_SN'},
				#   'comp_id2': {'HU_LENS'},
				#   'comp_id2': {'HU_LENS', 'HU_SN'},
				# }
				inv_map = {}
				comp_dict = {}
				for chan_name, comp_list in tran_data.compounds.items():
					for comp_obj in comp_list:
						comp_dict[comp_obj.id] = comp_obj
						if comp_obj.id in inv_map:
							inv_map[comp_obj.id].append(chan_name)
						else:
							inv_map[comp_obj.id] = [chan_name]

				for comp_id, chan_names in inv_map.items():

					if (len_uls == 0 and len([el for el in comp_dict[comp_id].comp if 'ul' in el]) > 0):
						self.logger.info(
							" -> LightCurve loading aborded for %s (upper limits required)" % 
							comp_id.hex()
						)
						continue

					lc = self.lcl.load_using_objects(comp_dict[comp_id], frozen_photo_dicts)

					# Associate it to the TransientData instance
					tran_data.add_lightcurve(chan_names, lc)

		# Feedback
		if verbose:
			self.feedback(tran_register.values(), photo_list, debug)

		return tran_register


	def feedback(self, dict_values, photo_list, debug):
		"""
		"""

		for tran_data in dict_values:

			len_comps = len({ell.id.hex() for el in tran_data.compounds.values() for ell in el})
			len_lcs = len({ell.id.hex() for el in tran_data.lightcurves.values() for ell in el})
			len_srs = len({id(ell) for el in tran_data.science_records.values() for ell in el})

			if photo_list is not None:

				pps = tran_data.photopoints
				uls = tran_data.upperlimits

				self.logger.info(
					"Transient %i loaded: PP: %i, UL: %i, CP: %i, LC: %i, SR: %i" % 
					(tran_data.tran_id, len(pps), len(uls), len_comps, len_lcs, len_srs)
				)

				if debug: 

					if len(pps) > 0:
						self.logger.info(
							"Photopoint(s): {}".format(
								(*pps,) if len(pps) > 1 else next(iter(pps))
							)
						)

					if len(uls) > 0:
						self.logger.info(
							"Upper limit(s): {}".format(
								(*uls,) if len(uls) > 1 else next(iter(uls))
							)
						)
			else:

				self.logger.info(
					"Transient %i loaded: PP: 0, UL: 0, CP: %i, LC: %i, SR: %i" % 
					(tran_data.tran_id, len_comps, len_lcs, len_srs)
				)

			if debug:
				
				if len_comps > 0:
					for channel in tran_data.compounds.keys():
						self.logger.info(
							"%s Compound(s): %s " %
							(
								"" if channel is None else "[%s]" % channel,
								[el.id.hex() for el in tran_data.compounds[channel]]
							)
						)


				if len_lcs > 0:
					for channel in tran_data.lightcurves.keys():
						self.logger.info(
							"%s LightCurves(s): %s " %
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
									"%s T2 %s: %s " %
									(
										"" if channel is None else "[%s]" % channel,
										t2_id, srs	
									)
								)
