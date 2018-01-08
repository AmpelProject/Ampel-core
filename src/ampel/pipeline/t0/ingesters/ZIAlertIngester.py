#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/t0/ingesters/ZIAlertIngester.py
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 14.12.2017
# Last Modified Date: 08.01.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>
import logging
from pymongo import UpdateOne, InsertOne, MongoClient
from pymongo.errors import BulkWriteError
from ampel.pipeline.t0.ingesters.AbstractIngester import AbstractIngester
from ampel.pipeline.t0.ingesters.utils.CompoundGenerator import CompoundGenerator
from ampel.pipeline.t0.ingesters.utils.T2DocsShaper import T2DocsShaper
from ampel.pipeline.t0.stampers.ZIPhotoPointStamper import ZIPhotoPointStamper
from ampel.pipeline.common.ChannelsConfig import ChannelsConfig
from ampel.flags.T2ModuleIds import T2ModuleIds
from ampel.flags.PhotoPointFlags import PhotoPointFlags
from ampel.flags.TransientFlags import TransientFlags
from ampel.flags.T2RunStates import T2RunStates
from ampel.flags.AlDocTypes import AlDocTypes
from ampel.flags.FlagUtils import FlagUtils
from ampel.flags.ChannelFlags import ChannelFlags

logger = logging.getLogger("Ampel")

# https://github.com/AmpelProject/Ampel/wiki/Ampel-Flags
SUPERSEEDED = FlagUtils.get_flag_pos_in_enumflag(PhotoPointFlags.SUPERSEEDED)
TO_RUN = FlagUtils.get_flag_pos_in_enumflag(T2RunStates.TO_RUN)

class ZIAlertIngester(AbstractIngester):
	"""
		Ingester class used by t0.AlertProcessor in 'online' mode.
		This class 'ingests' alerts (if they have passed the alert filter):
		it compares info between alert and DB and creates several documents 
		in the DB that are used in later processing stages (T2, T3)
	"""

	def __init__(self, mongo_client, channels_config, names_of_active_channels):
		"""
			mongo_client: (instance of pymongo.MongoClient) is required for database operations
		"""

		if not type(mongo_client) is MongoClient:
			import mongomock
			if not type(mongo_client) is mongomock.MongoClient:
				raise ValueError("The parameter mongo_client must be of type: MongoClient")

		if not type(channels_config) is ChannelsConfig:
			raise ValueError("The parameter channels_config must be of type: ampel.pipeline.common.ChannelsConfig")

		if not type(names_of_active_channels) is list:
			raise ValueError("The parameter names_of_active_channels must be of type: list")

		self.set_mongo(mongo_client)
		self.pps_stamper = ZIPhotoPointStamper()
		self.t2docs_shaper = T2DocsShaper(channels_config, names_of_active_channels)

		self.l_chanflag_pos = []
		self.l_chanflags = []

		for chan_name in names_of_active_channels:
			flag = channels_config.get_channel_flag_instance(chan_name)
			self.l_chanflags.append(flag)
			self.l_chanflag_pos.append(
				FlagUtils.get_flag_pos_in_enumflag(flag)
			)

		CompoundGenerator.cm_init_channels(channels_config, names_of_active_channels)


	def set_job_id(self, job_id):
		"""
			A dispatcher class creates/updates several documents in the DB for each alert.
			Among other things, it updates the main transient document, 
			which contains a list of jobIds associated with the processing of the given transient.
			We thus need to know what is the current jobId to perform this update.
			The provided parameter should be a mongoDB ObjectId.
		"""
		self.job_id = job_id


	def set_photopoints_stamper(self, arg_pps_stamper):
		"""
			Before the dispatcher instance inserts new photopoints into the photopoint collection, 
			it 'customizes' (or 'ampelizes' if you will) the photopoints in order to later enable
			the use of short and flexible queries. 
			The cutomizations are minimal, most of the original photopoint structure is kept.
			For exmample, in the case of ZIPhotoPointStamper:
				* The field candid is renamed in _id 
				* A new field 'alFlags' (AmpelFlags) is created (integer value of ampel.flags.PhotoPointFlags)
				* A new field 'alDocType' is created (integer value of ampel.flags.AlDocTypes.PHOTOPOINT)
			A photopoint stamper class (t0.pipeline.stampers.*) performs these operations.
			This method allows to customize the PhotoPointStamper instance to be used.
			By default, ZIPhotoPointStamper is used.
		"""
		self.pps_stamper = arg_pps_stamper


	def get_photopoints_stamper(self):
		"""
			Get the PhotoPointStamper instance associated with this class instance.
			For more information, please check the set_photopoints_stamper docstring
		"""
		return self.pps_stamper


	def set_mongo(self, mongo_client):
		"""
			Sets the mongo client (instance of pymongo.MongoClient) for database operations.
		"""
		self.db = mongo_client["Ampel"]
		self.col = self.db["main"]


	def ingest(self, tran_id, pps_alert, list_of_t2_modules):
		"""
			This method is called by t0.AmpelProcessor for 
			transients that pass at leat one T0 channel filter. 
			Then photopoints, transients and  t2 documents are pushed to the DB.
			A duplicate check is performed before DB insertions
		"""

		###############################################
		##   Part 1: Gather info from DB and alert   ##
		###############################################

		db_ops = []

		# TODO remove this for production
		pps_alert = [el for el in pps_alert if 'candid' in el and el['candid'] is not None]

		# Create set with pp ids from alert
		ids_pps_alert = {pp['candid'] for pp in pps_alert}

		# Evtly load existing photopoints from DB
		logger.info("Checking DB for existing pps")
		pps_db = list(
			self.col.find(
				{
					"tranId": tran_id, 
					"alDocType": AlDocTypes.PHOTOPOINT
				}, 
				{	
					"_id": 1, 
					"alFlags": 1, 
					"jd": 1, 
					"fid": 1, 
					"pid": 1,
					"alExcluded": 1
				}
			)
		)

		# Instanciate CompoundGenerator (used later for creating compounds and t2 docs)
		comp_gen = CompoundGenerator(pps_db, ids_pps_alert)

		# python set of ids from DB photopoints 
		ids_pps_db = comp_gen.get_db_ids()

		# If no photopoint exists in the DB, then this is a new transient 
		if not ids_pps_db:
			logger.info("Transient is new")



		###################################################
		##   Part 2: Check for reprocessed photopoints   ##
		###################################################

		# Difference between candids from db and candids from alert
		ids_in_db_not_in_alert = ids_pps_db - ids_pps_alert

		# If the set is not empty, either the transient is older that 30 days
		# or some photopoints were reprocessed
		if ids_in_db_not_in_alert:

			# Ignore ppts in db older than 30 days  
			min_jd = pps_alert[0]["jd"] - 30
			ids_in_db_older_than_30d = {pp["_id"] for pp in pps_db if pp["jd"] < min_jd }
			ids_flag_pp_as_superseeded = ids_in_db_not_in_alert - ids_in_db_older_than_30d

			# pps reprocessing occured at IPAC
			if ids_flag_pp_as_superseeded:

				# Match these with the photopoints from the alert
				for id_flag_pp_as_superseeded in ids_flag_pp_as_superseeded:

					pp_db_set_superseeded = next(
						filter(lambda x: x['_id'] == id_flag_pp_as_superseeded, pps_db)
					)

					for pp_alert in pps_alert:

						if (
							pp_db_set_superseeded["jd"] == pp_alert["jd"] and
							pp_db_set_superseeded["pid"] == pp_alert["pid"] and
							pp_db_set_superseeded["fid"] == pp_alert["fid"] 
						):

							logger.info(
								"Marking photopoint %s as superseeded by %s",
								pp_db_set_superseeded["_id"], 
								pp_alert['candid']
							)

							# Update set of superseeded ids (required for t2 & compounds doc creation)
							comp_gen.add_newly_superseeded_id(
								pp_db_set_superseeded["_id"]
							)

							db_ops.append(
								UpdateOne(
									{'_id': pp_db_set_superseeded["_id"]}, 
									{
										'$addToSet': {
											'newId': pp_alert['candid'],
											'alFlags': SUPERSEEDED
										}
									}
								)
							)
			else:
				logger.info("Transient has pps older than 30days")



		################################################
		##   Part 3: Insert new PhotoPoints into DB   ##
		################################################

		# Difference between candids from the alert and candids present in DB 
		ids_pps_to_insert = ids_pps_alert - ids_pps_db

		# If the photopoints already exist in DB 
		if not ids_pps_to_insert:
			logger.info("No new photo point to insert in DB")
		else:
			logger.info("Inserting %i new pp(s) into DB: %s" % (len(ids_pps_to_insert), ids_pps_to_insert))

			# Create of list photopoint dicts with photopoints matching the provided list of ids_pps_to_insert
			new_pps_dicts = [el for el in pps_alert if el['candid'] in ids_pps_to_insert]

			# ForEach 'new' photopoint (non existing in DB): 
			# Rename candid into _id, add tranId, alDocType and alFlags
			# Attention: this procedure *modifies* the dictionaries loaded by fastavro
			# (that's why you should not invert part 2 and 3 (in part 2, we access pp_alert['candid'] in
			# the case of IPAC reprocessing) unless you accept the performance penalty 
			# of copying (deep copy won't be necessary) the pp dicts from the alert)
			self.pps_stamper.stamp(tran_id, new_pps_dicts)

			# Insert new photopoint documents into 'photopoints' collection
			for pp in new_pps_dicts:
				db_ops.append(
					UpdateOne(
						{
							"_id": pp["_id"], 
						},
						{
							"$setOnInsert": pp
						},
						upsert=True
					)
				)
				



		#####################################################
		##   Part 4: Generate compound ids and compounds   ##
		#####################################################

		chan_flags = ChannelFlags(0)
		db_chan_flags = []

		for i, el in enumerate(list_of_t2_modules):
			if el is None:
				continue
			chan_flags |= self.l_chanflags[i]
			db_chan_flags.append(self.l_chanflag_pos[i])

		# Generate compound info for channels having passed T0 filters (i.e being not None)
		comp_gen.generate(chan_flags)

		# See how many different compound_id were generated (possibly a single one)
		# and generate corresponding ampel document to be inserted later
		for compound_id in comp_gen.get_compound_ids(chan_flags):
		
			d_addtoset = {
				"channels": {
					"$each": [
						FlagUtils.get_flag_pos_in_enumflag(flag)
						for flag in comp_gen.get_channels_for_compoundid(compound_id).as_list()
					]
				}
			}

			if comp_gen.has_flavors(compound_id):
				d_addtoset["flavors"] = {
					"$each": comp_gen.get_compound_flavors(compound_id) # returns a list
				}
			
			db_ops.append(
				UpdateOne(
					{
						"_id": compound_id, 
					},
					{
						"$setOnInsert": {
							"_id": compound_id,
							"alDocType": AlDocTypes.COMPOUND,
							"tranId": tran_id,
							"tier": 0,
							"pps": comp_gen.get_eff_compound(compound_id)
						},
						"$addToSet": d_addtoset
					},
					upsert=True
				)
			)
			


		#####################################
		##   Part 5: Generate t2 documents ##
		#####################################

		logger.debug("Generating T2 docs")
		ddd_t2_struct = self.t2docs_shaper.get_struct(
			comp_gen, list_of_t2_modules
		)
		
		# counter for user feedback (after next loop)
		db_ops_len = len(db_ops)

		# Loop over t2 modules
		for t2_id in ddd_t2_struct.keys():

			# Loop over parameter Ids
			for param_id in ddd_t2_struct[t2_id].keys():
			
				# Loop over compound Ids
				for compound_id in ddd_t2_struct[t2_id][param_id]:

					d_addtoset = {
						"channels": {
							"$each": [
								FlagUtils.get_flag_pos_in_enumflag(el) 
								for el in ddd_t2_struct[t2_id][param_id][compound_id].as_list()
							]
						}
					}

					if comp_gen.has_flavors(compound_id):
						d_addtoset["flavors"] = {
							"$each": comp_gen.get_t2_flavors(compound_id) # returns a list
						}

					db_ops.append(
						UpdateOne(
							{
								"tranId": tran_id, 
								"t2Module": FlagUtils.get_flag_pos_in_enumflag(t2_id), 
								"paramId": param_id, 
								"compoundId": compound_id,
							},
							{
								"$setOnInsert": {
									"tranId": tran_id,
									"alDocType": AlDocTypes.T2_RECORD,
									"t2Module": FlagUtils.get_flag_pos_in_enumflag(t2_id), 
									"paramId": param_id, 
									"compoundId": compound_id, 
									"runState": TO_RUN,
								},
								"$addToSet": d_addtoset
							},
							upsert=True
						)
					)

		# Insert generated t2 docs into collection
		logger.info("%i T2 docs will be inserted into DB", len(db_ops) - db_ops_len)



		############################################
		##   Part 6: Update transient documents   ##
		############################################

		# Insert/Update transient document into 'transients' collection
		logger.info("Updating transient document")

		# TODO add alFlags
		db_ops.append(
			UpdateOne(
				{
					"_id": tran_id
				},
				{
					"$setOnInsert": {
						"tranId": tran_id,
						"alDocType": AlDocTypes.TRANSIENT
					},
					'$addToSet': {
						'channels': {
							"$each": db_chan_flags
						},
						'jobIds': self.job_id
					},
					"$max": {
						"lastPPDate": pps_alert[0]["jd"]
					}
				},
				upsert=True
			)
		)

		try: 
			self.col.bulk_write(db_ops)
		except BulkWriteError as bwe: 
			logger.info(bwe.details) 
			# TODO add error flag to Job and Transient
			# TODO add return code 
