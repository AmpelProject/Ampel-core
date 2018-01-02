#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/t0/dispatchers/ZIAlertDispatcher.py
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 14.12.2017
# Last Modified Date: 02.01.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>
import logging, hashlib
from pymongo import UpdateOne, InsertOne
from pymongo.errors import BulkWriteError
from ampel.pipeline.t0.dispatchers.AbstractAmpelDispatcher import AbstractAmpelDispatcher
from ampel.pipeline.t0.stampers.ZIPhotoPointStamper import ZIPhotoPointStamper
from ampel.pipeline.t0.CompoundGenerator import CompoundGenerator
from ampel.flags.T2ModuleIds import T2ModuleIds
from ampel.flags.PhotoPointFlags import PhotoPointFlags
from ampel.flags.TransientFlags import TransientFlags
from ampel.flags.T2RunStates import T2RunStates
from ampel.flags.AlDocTypes import AlDocTypes
from ampel.flags.FlagUtils import FlagUtils

logger = logging.getLogger("Ampel")

# https://github.com/AmpelProject/Ampel/wiki/Ampel-Flags
SUPERSEEDED = FlagUtils.get_flag_position_in_enumflag(PhotoPointFlags.SUPERSEEDED)

class ZIAlertDispatcher(AbstractAmpelDispatcher):
	"""
		Dispatcher class used by t0.AlertProcessor in 'online' mode.
		This class re-route transient candidates into the NoSQL database
		if they have passed the configured filter.
	"""

	def __init__(self, mongo_client, db_config, channel_names):
		"""
			The parameter mongo_client (instance of pymongo.MongoClient) is required for database operations.
			Transient info will be stored the collection 'incoming' from the 'T0' database

			channel_names: list of channel names (associated properties are defined in the config DB document)
					       used by AlertProcessor (for example: ["SN", "LENS"])
		"""
		self.set_mongo(mongo_client)
		self.pps_stamper = ZIPhotoPointStamper()

		# create dd_full_t2Ids_paramIds_chanlist root dict
		self.dd_full_t2Ids_paramIds_chanlist = {}

		# self.dd_full_t2Ids_paramIds_chanlist:
		# --------------------------------
		#	
		# To insert t2 docs in a way that is efficient and not prone to race conditions
		# (see end part of the dispatch method) we need the following dict structure:
		#	
		# - SNCOSMO 
		# 	- default
		#		CHANNEL_SN|CHANNEL_LENS
		# 	- mySetting
		#		CHANNEL_GRB
		# - PHOTO_Z 
		# 	- default
		#		CHANNEL_SN|CHANNEL_LENS|CHANNEL_GRB
		#	
		# Each root entry in the dd_full_t2Ids_paramIds_chanlist dict is also a dict 
		# (with the different possible paramIds as key)
		#	
		# 1st dict key: t2 module id (integer)
		# 2nd key: t2 module paramId (string)
		#	
		# Values are combinations of channel flags

		# Loop through schedulable t2 modules 
		for t2_module_id in T2ModuleIds:

			# Each entry of dd_full_t2Ids_paramIds_chanlist is also a dict (1st dict key: t2 module id)
			self.dd_full_t2Ids_paramIds_chanlist[t2_module_id] = {}

			# Loop through the t0 channels 
			for t0_channel in channel_names:

				# Reset paramId
				paramId = None

				# Extract default parameter ID (paramId) associated with current T0 channel
				for registered_t2_module in db_config["T0"]["channels"][t0_channel]["t2Modules"]:
					
					# t2_module_id is of type 'enum', we access the flag label with the attribute 'name'
					if registered_t2_module["module"] == t2_module_id.name:
					
						# paramId is the name of the wished configuration set for this registered module
						paramId = registered_t2_module["paramId"]

				# if paramId was not found, it means the current t0_channel 
				# has not registered the current t2_module_id
				if paramId is None:
					continue
				
				# if paramId key was not yet stored in dd_full_t2Ids_paramIds_chanlist struct
				# create an empty ChannelFlags enum flag
				if not paramId in self.dd_full_t2Ids_paramIds_chanlist[t2_module_id]:
					self.dd_full_t2Ids_paramIds_chanlist[t2_module_id][paramId] = ChannelFlags(0)

				# Add current t0 channel to dd_full_t2Ids_paramIds_chanlist
				# For example: dd_full_t2Ids_paramIds_chanlist[SCM_LC_FIT]["default"] |= CHANNEL_SN
				self.dd_full_t2Ids_paramIds_chanlist[t2_module_id][paramId] |= ChannelFlags[
					db_config["T0"]["channels"][t0_channel]['flagLabel']
				]

		
		#CompoundGenerator.cm_set_channel_configs(
		#	[db_config["T0"]["channels"][t0_channel]['flagLabel'] for t0_channel in channel_names]
		#)


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


	def set_alertproc_channel_list(self, alertproc_channel_list):
		self.alertproc_channel_list = alertproc_channel_list
		CompoundGenerator.cm_set_channel_configs(
			[db_config["T0"]["channels"][t0_channel]['flagLabel'] for t0_channel in channel_names]
		)


	def set_mongo(self, mongo_client):
		"""
			Sets the mongo client (instance of pymongo.MongoClient) for database operations.
		"""
		self.db = mongo_client["Ampel"]
		self.col = self.db["main"]


	def dispatch(self, tran_id, pps_alert, array_of_scheduled_t2_modules):
		"""
			This method is called by t0.AmpelProcessor for 
			transients that pass at leat one T0 channel filter. 
			Then photopoints, transients and  t2 documents are pushed to the DB.
			A duplicate check is performed before DB insertions
		"""

		###############################################
		##   Part 0: Gather info from DB and alert   ##
		###############################################

		# TODO remove this for production
		pps_alert = [el for el in pps_alert if 'candid' in el and el['candid'] is not None]

		# Check existing photopoints in DB
		logger.info("Checking DB for existing pps")
		pps_db = self.col.find(
			{
				"tranId": tran_id, 
				"alDocType": AlDocTypes.PHOTOPOINT
			}, 
			{"_id": 1, "alFlags": 1}
		)

		# photopoint Ids from mongodb collection
		ids_pps_db = {pp["_id"] for pp in pps_db}

		# Photopoint Ids from alert
		ids_pps_alert = {pp['candid'] for pp in pps_alert}

		# Instanciate CompoundGenerator (used later for creating compounds and t2 docs)
		compound_gen = CompoundGenerator(pps_db, ids_pps_db, ids_pps_alert)

		# If no photopoint exists in the DB, then this is a new transient 
		if not ids_pps_db:
			logger.info("Transient is new")



		###################################################
		##   Part 1: Check for reprocessed photopoints   ##
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

				requests = []

				# Match these with the photopoints from the alert
				for id_flag_pp_as_superseeded in ids_flag_pp_as_superseeded:

					pp_db_set_superseeded = next(
						filter(lambda el: el['_id'] == id_flag_pp_as_superseeded, pps_db)
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
							compound_gen.add_newly_superseeded_id(
								pp_db_set_superseeded["_id"]
							)

							requests.append(
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

				try: 
					self.col.bulk_write(requests)
				except BulkWriteError as bwe: 
					logger.info(bwe.details) 
					# TODO add error flag to Job and Transient
					# TODO add return code 
			else:
				logger.info("Transient has pps older than 30days")



		################################################
		##   Part 2: Insert new PhotoPoints into DB   ##
		################################################

		# Difference between candids from the alert and candids present in DB 
		ids_pps_to_insert = ids_pps_alert - ids_pps_db

		# Avoid unnecessary recomputations of set difference needed by CompoundGenerator 
		compound_gen.set_db_inserted_ids(ids_pps_to_insert)

		# If the photopoints already exist in DB 
		if not ids_pps_to_insert:
			logger.info("No new photo point to insert in DB")
		else:
			logger.info("Inserting new pps: %s", ids_pps_to_insert)

			# Create of list photopoint dicts with photopoints matching the provided list of ids_pps_to_insert
			new_ppts_dicts = [el for el in pps_alert if el['candid'] in ids_pps_to_insert]

			# ForEach 'new' photopoint (non existing in DB): 
			# Rename candid into _id, add tranId, alDocType and alFlags
			# Attention: this procedure *modifies* the dictionaries loaded by fastavro
			# (that's why you should not invert part 1 and 2 (in part 1, we access pp_alert['candid'] in
			# the case of IPAC reprocessing) unless you accept the performance penalty 
			# of copying (deep copy won't be necessary) the pp dicts from the alert)
			self.pps_stamper.stamp(tran_id, new_ppts_dicts)

			# Insert new photopoint documents into 'photopoints' collection
			self.col.insert_many(new_ppts_dicts)



		####################################################
		##   Part 3: Generate t2 and compound documents   ##
		####################################################

		compoundId = hashlib.md5(bytes(hash_payload, "utf-8")).hexdigest()

		logger.info("Generated compoundId: %s", compoundId)


		logger.debug("Generating T2 docs")
		dd_eff_t2s_paramIds_chanlist = {}


		# ----------------------------------------------------------------------------------
		# The following task is bit complex, so is the associated explanation, sorry for that.
		# ----------------------------------------------------------------------------------
		#	
		# - On the one side, we have scheduled T2s for each "active" channel, say:
		#
		# CHANNEL_SN: SNCOSMO & PHOTO_Z
		# CHANNEL_GRB: GRB_FIT & PHOTO_Z & SNCOSMO 
		#
		#     But since T0 filters can 'customize' scheduled T2s, we could have alternatively:
		#     CHANNEL_SN: SNCOSMO 
		#     CHANNEL_GRB: GRB_FIT & PHOTO_Z 
		#	
		# ----------------------------------------------------------------------------------
		#	
		# - On the other side, we have self.dd_full_t2Ids_paramIds_chanlist 
		#   dynamically created for every possible channel based on the ampel config entries:
		#	
		# - SNCOSMO 
		# 	- default
		#		CHANNEL_SN|CHANNEL_LENS
		# 	- mySetting
		#		CHANNEL_GRB
		# - PHOTO_Z 
		# 	- default
		#		CHANNEL_SN|CHANNEL_LENS|CHANNEL_GRB
		# - GRB_FIT:
		#   - default
		#       CHANNEL_GRB
		#	
		# ----------------------------------------------------------------------------------
		#	
		# The following section matches array_of_scheduled_t2_modules with self.dd_full_t2Ids_paramIds_chanlist 
		# in order to create dd_eff_t2s_paramIds_chanlist (eff=effective) which - following the example with 
		# 'customized' scheduled T2s - would look like this:
		#	
		# - SNCOSMO 
		# 	- default
		#		CHANNEL_SN
		# - PHOTO_Z 
		# 	- default
		#		CHANNEL_SN|CHANNEL_GRB
		# - GRB_FIT:
		#   - default
		#       CHANNEL_GRB
		#	
		# ----------------------------------------------------------------------------------
	

		# loop through all channels, 
		# get scheduled T2s (single_channel_scheduled_t2s) for each channel (self.alertproc_channel_list[i])
		for i, single_channel_scheduled_t2s in enumerate(array_of_scheduled_t2_modules):

			# Skip Nones (current channel with index i has rejected this transient)
			if single_channel_scheduled_t2s is None:
				continue

			# loop through schedulable module ids (T2ModuleIds enum)
			# (the enum flag instance single_channel_scheduled_t2s is not iterable)
			# Therefore we loop over T2ModuleIds and check which flag is in single_channel_scheduled_t2s
			for t2_module_id in T2ModuleIds:

				# consider only t2 modules scheduled by this channel (single_channel_scheduled_t2s)
				if not t2_module_id in single_channel_scheduled_t2s:
					continue
			
				# Create dict instance if necessary	
				if not t2_module_id in dd_eff_t2s_paramIds_chanlist:
					dd_eff_t2s_paramIds_chanlist[t2_module_id] = {}

				# loop through all known paramIds for this t2 module
				for paramId in self.dd_full_t2Ids_paramIds_chanlist[t2_module_id].keys():
				
					# If the transientFlag of the current channel (index i)
					# is registered in dd_full_t2Ids_paramIds_chanlist
					if self.alertproc_channel_list[i] in self.dd_full_t2Ids_paramIds_chanlist[t2_module_id][paramId]:

						if not paramId in dd_eff_t2s_paramIds_chanlist[t2_module_id]:
							dd_eff_t2s_paramIds_chanlist[t2_module_id][paramId] = []

						dd_eff_t2s_paramIds_chanlist[t2_module_id][paramId].append(
							self.alertproc_channel_list[i].value
						)


		# Create T2 documents
		#####################

		# Loop over t2 modules
		for t2_module in dd_eff_t2s_paramIds_chanlist.keys():

			# Loop over parameter Ids
			for paramId in dd_eff_t2s_paramIds_chanlist[t2_module].keys():
			
				requests.append(
					UpdateOne(
						{
							"t2Module": t2_module.value, 
							"paramId": paramId, 
							"compoundId": compoundId,
						},
						{
							"$setOnInsert": {
								"tranId": tran_id,
								"t2Module": t2_module.value, 
								"paramId": paramId, 
								"compoundId": compoundId, 
								"compound": compound,
								"runState": T2RunStates.TO_RUN,
							},
							"$addToSet": {
								"channels": {
									"$each": dd_eff_t2s_paramIds_chanlist[t2_module][paramId]
								}
							}
						},
						upsert=True
					)
				)

		# Insert generated t2 docs into collection
		logger.info("Inserting %i T2 docs into DB", len(requests))

		try: 
			self.col.bulk_write(requests)
		except BulkWriteError as bwe: 
			logger.info(bwe.details) 
			# TODO add error flag to Job and Transient



		# Insert/Update transient document into 'transients' collection
		logger.info("Updating transient document")

		# TODO add alFlags
		self.col.update_one(
			{"_id": tran_id},
			{
				'$addToSet': {
					'channels': {
						"$each": [
							self.alertproc_channel_list[i].value
							for i, el in enumerate(array_of_scheduled_t2_modules) 
							if el is not None
						]
					},
					'jobIds': self.job_id
				},
				"$max": {
					"lastPPDate": pps_alert[0]["jd"]
				}
			},
			upsert=True
		)

