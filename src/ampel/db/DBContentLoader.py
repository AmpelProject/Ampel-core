#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/db/DBContentLoader.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 13.01.2018
# Last Modified Date: 11.10.2019
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from bson import ObjectId
from pkg_resources import iter_entry_points

from ampel.object.ScienceRecord import ScienceRecord
from ampel.object.PlainPhotoPoint import PlainPhotoPoint
from ampel.object.PlainUpperLimit import PlainUpperLimit
from ampel.object.Compound import Compound

from ampel.core.flags.AlDocType import AlDocType
from ampel.core.flags.T2RunStates import T2RunStates

from ampel.logging.LoggingUtils import LoggingUtils
from ampel.logging.AmpelLogger import AmpelLogger
from ampel.common.AmpelUtils import AmpelUtils
from ampel.db.AmpelDB import AmpelDB
from ampel.t2.loader.LightCurveLoader import LightCurveLoader
from ampel.db.query.QueryLatestCompound import QueryLatestCompound
from ampel.db.query.QueryLoadT2Info import QueryLoadT2Info
from ampel.t3.TransientData import TransientData
from ampel.config.AmpelConfig import AmpelConfig
from ampel.config.utils.LogicSchemaUtils import LogicSchemaUtils

class DBContentLoader:
	"""
	"""
	all_doc_types = (
		AlDocType.PHOTOPOINT, AlDocType.UPPERLIMIT, AlDocType.COMPOUND, 
		AlDocType.TRANSIENT, AlDocType.T2RECORD,
	)

	def __init__(self, verbose=True, debug=False, logger=None):
		"""
		"""
		self.logger = AmpelLogger.get_logger() if logger is None else logger
		self.lcl = LightCurveLoader(logger=self.logger)
		self.rev_tags = self.lcl.rev_tags # shortcut
		self.col_stock = AmpelDB.get_collection('stock')
		self.col_t0 = AmpelDB.get_collection("t0")
		self.col_t1 = AmpelDB.get_collection("t1")
		self.col_t2 = AmpelDB.get_collection("t2")
		self.ext_journal_col = AmpelDB.get_collection("journal")
		self.verbose = verbose
		self.debug = debug

		if self.debug:
			self.verbose = True


	def load_new(
		self, tran_id, channels=None, state_op="$latest", 
		states=None, docs=all_doc_types, t2_subsel=None
	):
		"""
		:type tran_id: int, list(int)
		:param tran_id: transient id. Can be a multiple IDs (list) \
		if state_op is '$all' or states are provided 

		:type channels: str, dict
		:param channels: string (one channel only) or a dict \
		(see :obj:`QueryMatchSchema <ampel.db.query.QueryMatchSchema>` \
		for syntax details). None (no criterium) means all channels are considered. 

		:param str state_op:\n
			- "$latest": latest state will be retrieved
			- "$all": all states present in DB (at execution time) will be retrieved

		:type states: str, bytes, bson.binary.Binary
		:param states: <compound_id> or list of <compound_id>: \
		provided state(s) will be loaded. Each compound id must be either:\n
			- a 32 alphanumerical string
			- 16 bytes (128 bits)
			- a bson.binary.Binary instance with subtype 5

		:param List[AlDocType] docs: list of AlDocType enum members:

		  * TRANSIENT: 
			| -> Add info from DB doc to the returned TransientData instance
			| -> For example: channels, tags (has processing errors), 
			   latest photopoint observation date, ...

		  * PHOTOPOINT: 
			| -> load *all* photopoints avail for this transient (regardless of provided state)
			| -> The transient will contain a list of ampel.base.PlainPhotoPoint instances 
			| -> (PlainPhotoPoint, unlike PhotoPoint, instances come without policy)

		  * UPPERLIMIT: 
			| -> load *all* upper limits avail for this transient (regardless of provided state)
			| -> The transient will contain a list of ampel.base.PlainUpperLimit instances 
			| -> (PlainUpperLimit, unlike UpperLimit, instances come without policy)

		  * COMPOUND: 
			| -> ampel.base.LightCurve instances are created based on DB documents 
			   (with alDocType AlDocType.COMPOUND)
			| -> if 'state' is '$latest' or a state id (md5 bytes) is provided, 
			   only one LightCurve instance is created. 
			   if 'state' is '$all', all available lightcurves are created.
			| -> the lightcurve instance(s) will be associated with the returned TransientData instance

		  * T2RECORD: 
			...

		:type t2_subsel: str, list(str)
		:param t2_subsel: optional sub-selection of t2 results based on t2 unit id(s). \
		t2_subsel will *include* the provided results of t2s with the given ids \
		and thus exclude other t2 results. If None or an empty list is provided: \
		all t2 docs associated with the matched transients will be loaded. \
		A single t2 unit id (string) can be provided.

		:returns: list of TransientData instances
		:rtype: list(:py:class:`TransientData <ampel.t3.TransientData>`)
		"""

		if not docs:
			raise ValueError("Invalid 'docs' parameter")

		extra = {'tranId': tran_id}
		db_docs = {}

		if AlDocType.T2RECORD in docs or AlDocType.COMPOUND in docs:

			# Option 1: Load a t2s for user provided state(s)
			if states is not None:

				# Feedback
				self.logger.info(
					"Retrieving docs associated with provided state%s" %
					"s" if len(states) > 1 else ""
				)

				if AlDocType.T2RECORD in docs:

					# Build query
					t2_query = QueryLoadT2Info.build_statebound_query(
						tran_id, states, channels, t2_subsel
					)

			else:

				# Option 2: Find latest state, then update search query parameters
				if state_op == "$latest":
		
					if isinstance(tran_id, (list, tuple)):
						raise ValueError(
							"Querying multiple transients is not supported with state_op '$latest'"
						)
			
					# Note1 : we use general_query on a single transient. 
					# Note2: T3Event works more efficiently, it determines latest states 
					# for multiple transients at once and calls this method with those states.
					latest_state_query = QueryLatestCompound.general_query(tran_id)

					# Feedback
					self.logger.debug(
						"Determining latest state", extra={
							'tranId': tran_id,
							'query': LoggingUtils.safe_query_dict(
								latest_state_query, dict_key=None
							)
						}
					)

					latest_state_result = self.col_t1.aggregate(
						latest_state_query
					)
		
					# Execute DB query 
					latest_compound = next(latest_state_result, None)
		
					if latest_compound is not None:
	
						if AlDocType.T2RECORD in docs:

							# Build query parameters (will return adequate number of docs)
							t2_query = QueryLoadT2Info.build_statebound_query(
								tran_id, latest_compound["_id"], channels, t2_subsel
							)

						if AlDocType.COMPOUND in docs:
							db_docs['t1'] = [latest_compound]
		
				# Option 3: Load t2s for every available transient state
				elif state_op == "$all":
		
					# Build query parameters (will return adequate number of docs)
					self.logger.debug("State operator: $all")
					t2_query = QueryLoadT2Info.build_stateless_query(
						tran_id, channels, t2_subsel
					)

			# Record query
			self.logger.debug(
				None, extra={
					'tranId': tran_id,
					'query': LoggingUtils.safe_query_dict(
						t2_query, dict_key=None
					)
				}
			)

			# Execute DB query
			db_docs['t2'] = list(
				self.col_t2.find(t2_query)
			)

		lookup_id = {'$in': tran_id} if isinstance(tran_id, (list, tuple)) else tran_id

		# Photo DB query 
		if AlDocType.PHOTOPOINT in docs and AlDocType.UPPERLIMIT in docs:
			db_docs['t0'] = list(
				self.col_t0.find({'tranId': lookup_id})
			)

		else:

			if AlDocType.PHOTOPOINT in docs:
				db_docs['t0'] = list(
					self.col_t0.find({'_id': {'$gt': 0}, 'tranId': lookup_id})
				)

			if AlDocType.UPPERLIMIT in docs:
				db_docs['t0'] = list(
					self.col_t0.find({'_id': {'$lt': 0}, 'tranId': lookup_id})
				)

		if AlDocType.TRANSIENT in docs:
			db_docs['ext_journal'] = list(
				self.ext_journal_col.find({'_id': lookup_id})
			)
			#list_tran = list(
			#	self.col_stock.find({'_id': lookup_id})
			#)

		if 't0' in db_docs and not TransientData.data_access_controllers:
			for el in iter_entry_points('ampel.sources'):
				TransientData.add_data_access_controller(
					el.load().get_data_access_controller()
				)

		self.logger.info(
			"Fetched docs: " + ", ".join([
				"%s: %s" % (el, str(len(db_docs[el]))) 
				for el in ("t0", "t1", "t2", 'stock', "ext_journal")
			]), 
			extra=extra
		)

		# key: tran_id, value: instance of TransientData
		dict_tran_data = self.create_tran_data(
			db_docs, channels=channels, 
			state_op=state_op, extra=extra
		)

		if dict_tran_data is None:
			return []

		return dict_tran_data.values()


	def create_tran_data(
		self, db_docs=None, channels=None, state_op=None, 
		load_lightcurves=True, extra=None
	):
		"""
		:returns: dict with key: tran_id, value: instance of :py:class:`TransientData <ampel.t3.TransientData>`
		:rtype: Dict[int, TransientData]
		"""

		self.logger.info("Creating TransientData", extra=extra)

		# Build set: we need intersections later
		channels_set = LogicSchemaUtils.reduce_to_set(channels) if channels else None

		# Stores loaded transient items. 
		# Key: tran_id, value: TransientData instance
		tran_register = {}
		tran_id = ""

		# Loop through transient docs
		if 'stock' in db_docs:

			for doc in db_docs['stock']:

				if doc['_id'] not in tran_register:
					tran_data = TransientData(doc['_id'], state_op, self.logger)
					tran_register[doc['_id']] = tran_data
				else:
					tran_data = tran_register[doc['_id']]

				tran_data.set_tags(doc['alTags'])

				# Set transient names (ex: ZTF18acdzzyf) 
				# tranNames is a list as TNS name and other kind of names may be added later
				if 'tranNames' in doc:
					tran_data.set_tran_names(doc['tranNames'])

				# Use all avail channels if no channel query constraint was used, 
				# otherwise: intersection
				tran_data.set_channels( # Transient doc['channels'] cannot be None
					AmpelUtils.to_set(doc['channels']) if channels_set is None
					else (channels_set & AmpelUtils.to_set(doc['channels'])) # intersection
				)

				# Save journal entries associated with provided channels
				self.import_journal(tran_data, doc['journal'], channels_set)


		# Loop through compound docs
		if "t1" in db_docs:
			
			for doc in db_docs["t1"]:

				tran_id = doc['tranId']

				if tran_id in tran_register:
					tran_data = tran_register[tran_id]
				else:
					tran_data = TransientData(tran_id, state_op, self.logger)
					tran_register[tran_id] = tran_data

				doc['id'] = doc.pop('_id')
				doc_chans = doc.pop('channels')
				doc['alTags'] = [
					self.rev_tags[el] if el in self.rev_tags else el for el in doc['alTags']
				]

				comp_chans = doc_chans if channels is None else (channels_set & set(doc_chans))

				tran_data.add_compound(
					comp_chans, # intersection
					Compound(**AmpelConfig.recursive_freeze(doc)),
				)

				if len(comp_chans) == 1 and state_op == "$latest":
					tran_data.set_latest_state(comp_chans, doc['id'])


		if 't2' in db_docs:

			# Loop through t2 docs
			for doc in db_docs['t2']:
				
				tran_id = doc['tranId']

				if tran_id in tran_register:
					tran_data = tran_register[tran_id]
				else:
					tran_data = TransientData(tran_id, state_op, self.logger)
					tran_register[tran_id] = tran_data

				science_record = ScienceRecord(
					doc['tranId'],
					doc['t2UnitId'],
					doc['docId'],
					doc.get('results'),
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

		# Loop through photometric data
		if 't0' in db_docs:

			loaded_tran_ids = tran_register.keys()

			# Share common upper limits among different Transients
			loaded_uls = {}

			# Loop through photo results
			for doc in db_docs['t0']:

				# Generate PhotoFlags
				photo_flag = [self.rev_tags[el] if el in self.rev_tags else el for el in doc['alTags']]

				# Pick photo point dicts
				if doc["_id"] > 0:
					
					# Photopoints instance attached to the transient instance 
					# are not bound to a compound and come thus without policy
					if isinstance(doc['tranId'], list):
						for tran_id in (loaded_tran_ids & doc['tranId']):
							tran_register[tran_id].add_photopoint(
								PlainPhotoPoint(doc, photo_flag, read_only=True)
							)
					else:
						tran_register[doc['tranId']].add_photopoint(
							PlainPhotoPoint(doc, photo_flag, read_only=True)
						)

				# Pick upper limit dicts
				else:

					# UpperLimits instance attached to the transient instance 
					# are not bound to a compound and come thus without policy 
					if isinstance(doc['tranId'], int):
						tran_register[doc['tranId']].add_upperlimit(
							PlainUpperLimit(doc, photo_flag, read_only=True)
						)
					
					else: # list
						
						doc_id = doc["_id"]
						for tran_id in (loaded_tran_ids & doc['tranId']):
							if doc_id not in loaded_uls:
								loaded_uls[doc_id] = PlainUpperLimit(
									doc, photo_flag, read_only=True
								)
							tran_register[tran_id].add_upperlimit(loaded_uls[doc_id])

		# Import ext journal entries
		if 'ext_journal' in db_docs:

			for doc in db_docs['ext_journal']:

				self.import_journal(
					tran_register[doc['_id']], 
					doc['journal'], channels_set
				)

		# Load lightcurve objects if all ingredients were loaded
		if load_lightcurves and 't0' in db_docs:
		
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
							"LightCurve loading aborded for %s (upper limits required)" % comp_id.hex(), 
							extra=extra
						)
						continue

					lc = self.lcl.load_using_objects(comp_dict[comp_id], frozen_photo_dicts)

					# Associate it to the TransientData instance
					tran_data.add_lightcurve(chan_names, lc)

		# Feedback
		if self.verbose:
			self.feedback(tran_register.values(), db_docs['t0'])

		return tran_register


	def import_journal(self, tran_data, journal_entries, channels_set):
		"""
		:param tran_data: instance of TransientData
		:type tran_data: :py:class:`TransientData <ampel.t3.TransientData>`
		:param list(Dict) journal_entries:
		:param channels_set:
		:type channels_set: set(str), str
		"""

		# Save journal entries related to provided channels
		for entry in journal_entries:

			# Not sure if those entries will exist. Time will tell.
			if entry.get('channels') is None:
				self.logger.warn(
					'Ignoring following channel-less journal entry: %s' % str(entry), 
					extra={'tranId': tran_data.tran_id}
				)
				continue

			if channels_set == "Any":
				chans_intersec = "Any"
			else:
				# Set intersection between registered and requested channels (if any)
				chans_intersec = (
					entry['channels'] if channels_set is None 
					else (channels_set & AmpelUtils.to_set(entry['channels']))
				)

			# Removing embedded 'channels' key/value and add journal entry 
			# to transient data while maintaining the channels association
			tran_data.add_journal_entry(
				chans_intersec,
				# journal entry without the channels key/value
				{k:v for k, v in entry.items() if k != 'channels'}	
			)



	def feedback(self, dict_values, list_t0):
		"""
		"""

		for tran_data in dict_values:

			len_comps = len({ell.id.hex() for el in tran_data.compounds.values() for ell in el})
			len_lcs = len({ell.id.hex() for el in tran_data.lightcurves.values() for ell in el})
			len_srs = len({id(ell) for el in tran_data.science_records.values() for ell in el})
			pps = tran_data.photopoints
			uls = tran_data.upperlimits

			extra = {
				'tranId': tran_data.tran_id,
				'channels': AmpelUtils.try_reduce(tran_data.channels)
			}

			if not isinstance(extra['channels'], str):
				# convert set to list (otherwise pymongo complains)
				if isinstance(extra['channels'], int):
					extra['channels'] = [extra['channels']]
				else:
					extra['channels'] = list(extra['channels'])

			if self.debug:

				if list_t0:
					if len(pps) > 0:
						extra['pp'] = AmpelUtils.try_reduce(tuple(pps.keys()))

					if len(uls) > 0:
						extra['ul'] = AmpelUtils.try_reduce(tuple(uls.keys()))
				
				if len_comps > 0:

					if len(tran_data.channels) > 1:
						extra['cp'] = {}
						for channel in tran_data.compounds.keys():
							extra['cp'][channel] = AmpelUtils.try_reduce(
								[el.id for el in tran_data.compounds[channel]]
							)
					else:
						extra['cp'] = AmpelUtils.try_reduce(
							[el.id for el in next(iter(tran_data.compounds.values()))]
						)

				if len_lcs > 0:

					if len(tran_data.channels) > 1:
						extra['lc'] = {}
						for channel in tran_data.lightcurves.keys():
							extra['lc'][channel] = AmpelUtils.try_reduce(
								[el.id for el in tran_data.lightcurves[channel]]
							)
					else:
						extra['lc'] = AmpelUtils.try_reduce(
							[el.id for el in next(iter(tran_data.lightcurves.values()))]
						)

				if len_srs > 0:

					# All T2 ids avail for this tranData
					subsel = {ell.t2_unit_id for el in tran_data.science_records.values() for ell in el}

					extra['sr'] = {}
					for channel in tran_data.science_records.keys():

						for t2_id in subsel:

							# Count # of science record for this t2id and channel
							srs = len(list(filter(
								lambda x: x.t2_unit_id == t2_id,
								tran_data.science_records[channel]
							)))

							if srs > 0:
					
								if len(tran_data.channels) > 1:
									if channel not in extra['sr']:
										extra['sr'][channel] = {}
									extra['sr'][channel][t2_id] = srs
								else:
									extra['sr'][t2_id] = srs

			self.logger.verbose(
				"TranData: PP: %i, UL: %i, CP: %i, LC: %i, SR: %i" % 
				(len(pps), len(uls), len_comps, len_lcs, len_srs),
				extra=extra
			)
