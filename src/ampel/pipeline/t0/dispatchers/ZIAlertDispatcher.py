#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/t0/dispatchers/ZIAlertDispatcher.py
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 14.12.2017
# Last Modified Date: 29.12.2017
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>
import logging, hashlib
from pymongo import UpdateOne, InsertOne
from pymongo.errors import BulkWriteError
from ampel.pipeline.t0.dispatchers.AbstractAmpelDispatcher import AbstractAmpelDispatcher
from ampel.pipeline.t0.stampers.ZIPhotoPointStamper import ZIPhotoPointStamper
from ampel.flags.T2ModuleIds import T2ModuleIds
from ampel.flags.PhotoPointFlags import PhotoPointFlags
from ampel.flags.TransientFlags import TransientFlags
from ampel.flags.RunStates import RunStates

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
		"""
		self.logger = logging.getLogger("Ampel")
		self.set_mongo(mongo_client)
		self.pps_stamper = ZIPhotoPointStamper()

		# create t2s_params_channels root dict
		self.t2s_params_channels = {}

		# t2s_params_channels:
		# -------------------
		#	
		# To insert t2 docs in a way that is efficient and not prone to race conditions
		# (see end part of the dispatch method) we need the following dict structure:
		#	
		# - SCM_LC_FIT
		# 	- default
		#		CHANNEL_SN|CHANNEL_LENS
		# 	- mySetting
		#		CHANNEL_GRB
		# - SCM_PHOTO_Z 
		# 	- default
		#		CHANNEL_SN|CHANNEL_LENS|CHANNEL_GRB
		#	
		# Each root entry in the t2s_params_channels dict is also a dict 
		# (with the different possible paramIds as key)
		#	
		# 1st dict key: t2 module id (integer)
		# 2nd key: t2 module paramId (string)
		#	
		# Values are combinations of T2ModuleIds flags

		# Loop through schedulable t2 modules 
		for t2_module_id in T2ModuleIds:

			# Each entry of t2s_params_channels is also a dict (1st dict key: t2 module id)
			self.t2s_params_channels[t2_module_id] = {}

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
				
				# if paramId key was not yet used in t2s_params_channels struct, create empty T2ModuleIds flag
				if not paramId in self.t2s_params_channels[t2_module_id]:
					self.t2s_params_channels[t2_module_id][paramId] = T2ModuleIds(0)

				# Add current t0 channel to t2s_params_channels
				# For example: t2s_params_channels[SCM_LC_FIT]["default"] |= CHANNEL_SN
				self.t2s_params_channels[t2_module_id][paramId] |= T2ModuleIds[
					db_config["T0"]["channels"][t0_channel]['flagLabel']
				]


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
			the use of performant/flexible queries. 
			The cutomizations are minimal, most of the original photopoint structure is kept.
			For exmample, in the case of ZIPhotoPointStamper:
				* The field candid is renamed in _id 
				* A new field 'alFlags' (AmpelFlags) is created (integer value of ampel.flags.PhotoPointFlags)
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


	def map_channel_to_transient_flag(self, transient_flag_list):
		self.channel_tranflag_map = transient_flag_list


	def set_mongo(self, mongo_client):
		"""
			Sets the mongo client (instance of pymongo.MongoClient) for database operations.
		"""
		self.db = mongo_client["Ampel"]
		self.col_pps = self.db["photopoints"]
		self.col_tran = self.db["transients"]
		self.col_t2 = self.db["t2"]


	def dispatch(self, tran_id, alert_pps_list, all_channels_t2_flags, force=False):
		"""
			This method is called by t0.AmpelProcessor for 
			transients that pass at leat one T0 channel filter. 
			Then photopoints, transients and  t2 documents are pushed to the DB.
			A duplicate check is performed before DB insertions
		"""

		# TODO remove this for production
		alert_pps_list = [el for el in alert_pps_list if 'candid' in el and el['candid'] is not None]

		# All candids from the alert
		ppt_ids_in_alert = {el['candid'] for el in alert_pps_list}

		# Check existing photopoints in DB
		self.logger.info("Checking DB for existing ppts")
		ppts_in_db = self.col_pps.find(
			{"tranId": tran_id}, 
			{"_id": 1, "alFlags": 1}
		)

		ppt_ids_in_db = set()
		ppt_excluded_ids = set()
		wzm_ids = set()

		# If no photopoint exists in the DB, then this is a new transient 
		if ppts_in_db.count() == 0:
			ppt_ids_to_insert = ppt_ids_in_alert
			self.logger.info("Transient is new")
		else:

			for ppt_db in ppts_in_db:

				ppt_ids_in_db.add(ppt_db["_id"])
				alFlags = PhotoPointFlags(ppt_db["alFlags"])

				if PhotoPointFlags.PP_EXCLUDE in alFlags:
					self.logger.info("Following PPT is marked for exclusion: %s", ppt_db["_id"])
					ppt_excluded_ids.add(ppt_db["_id"])
				if PhotoPointFlags.HAS_WEIZMANN_PHOTO in alFlags:
					self.logger.info("Following PPT has WZM photometry: %s", ppt_db["_id"])
					wzm_ids.add(ppt_db["_id"])

			# Difference between candids from the alert and candids present in DB 
			ppt_ids_to_insert = ppt_ids_in_alert - ppt_ids_in_db

		# If the photopoints already exist in DB (check T2 documents ?) 
		if not ppt_ids_to_insert:
			self.logger.info("No photo point to dispatch")
			if force is False:
				return

		# Difference between candids from db and candids from alert
		db_ppt_ids_not_in_alert = ppt_ids_in_db - ppt_ids_in_alert

		# If the set is not empty, either the transient is older that 30 days
		# or some photopoints were reprocessed
		if db_ppt_ids_not_in_alert:

			# Ignore ppts in db older than 30 days  
			db_ppt_ids_not_in_alert -= {
				el["_id"] for el in self.col_pps.find(
					{"transId": tran_id, "jd": {"$lt": alert_pps_list[0]["jd"] - 30}}, 
					{"_id": 1}
				)
			}

			# If db_ppt_ids_not_in_alert set is not empty, ppts reprocessing occured at IPAC
			if db_ppt_ids_not_in_alert:

				# Get photopoints younger than 30 days, that exist in the DB and not in the alert
				superseeded_db_ppts = list(
					self.col_pps.find(
						{"_id": {"$in": list(db_ppt_ids_not_in_alert)}}
					)
				)

				requests = []

				# Match these with the photopoints from the alert
				for superseeded_db_ppt in superseeded_db_ppts:
					for ppt_in_alert in alert_pps_list:
						if (
							superseeded_db_ppt["jd"] == ppt_in_alert["jd"] and
							superseeded_db_ppt["pid"] == ppt_in_alert["pid"] and
							superseeded_db_ppt["fid"] == ppt_in_alert["fid"] 
						):

							self.logger.info(
								"Marking ppt %s as superseeded by %s",
								superseeded_db_ppt["_id"], 
								ppt_in_alert['candid']
							)

							# Update set of excluded ids (will be used when creating t2 docs)
							ppt_excluded_ids.add(superseeded_db_ppt["_id"])

							requests.append(
								UpdateOne(
									{'_id': superseeded_db_ppt["_id"]}, 
									{
										'$addToSet': {
											'newid': ppt_in_alert['candid'],
										},
										'$set': {
											'alFlags':
												(PhotoPointFlags(superseeded_db_ppt['alFlags']) | 
												PhotoPointFlags.PP_SUPERSEEDED | 
												PhotoPointFlags.PP_EXCLUDE).value
										}
									}
								)
							)

				try: 
					self.col_pps.bulk_write(requests)
				except BulkWriteError as bwe: 
					self.logger.info(bwe.details) 
					# TODO add error flag to Job and Transient
			else:
				self.logger.info("Transient has PPTs older than 30days")

		# Create of list photopoint dicts with photopoints matching the provided list of ppt_ids_to_insert
		new_ppts_dicts = [el for el in alert_pps_list if el['candid'] in ppt_ids_to_insert]

		# Set _id to candid, append tran_id, flag each photopoint
		self.pps_stamper.stamp(tran_id, new_ppts_dicts)

		# Insert new photopoint documents into 'photopoints' collection
		self.logger.info("Inserting new ppts: %s", ppt_ids_to_insert)
		self.col_pps.insert_many(new_ppts_dicts)
			


		# Create compoundId
		compound = []
		requests = []
		hash_payload = ""

		# If this is a new transient 
		if ppts_in_db.count() == 0:
			for ppt_id in sorted(ppt_ids_to_insert):
				hash_payload += '%i' % ppt_id
				compound.append({'ppt': ppt_id})
		else:
			for ppt_id in sorted(set().union(ppt_ids_in_db, ppt_ids_in_alert) - ppt_excluded_ids):
				hash_payload += '%i' % ppt_id
				if ppt_id in wzm_ids:
					hash_payload += "wzm:1"
					compound.append({'ppt': ppt_id, 'wzm': 1})
				else:
					compound.append({'ppt': ppt_id})

		compoundId = hashlib.md5(bytes(hash_payload, "utf-8")).hexdigest()

		self.logger.info("Generated compoundId: %s", compoundId)


		self.logger.debug("Generating T2 docs")
		dict_t2_modules = {}

		# loop through all channels, 
		# get schedulings flags for channel "single_channel_t2_flags" 
		for i, single_channel_t2_flags in enumerate(all_channels_t2_flags):

			# Skip Nones
			if single_channel_t2_flags is None:
				continue

			# loop through schedulable module ids
			for t2_module_id in T2ModuleIds:

				# Ignore scheduling flags not set for this channel
				if not t2_module_id in single_channel_t2_flags:
					continue
				
				if not t2_module_id in dict_t2_modules:
					dict_t2_modules[t2_module_id] = {}

				# loop through all known paramIds for this t2 module
				for paramId in self.t2s_params_channels[t2_module_id].keys():
				
					# If the transientFlag of the current channel (index i)
					# is registered in t2s_params_channels
					if self.channel_tranflag_map[i] in self.t2s_params_channels[t2_module_id][paramId]:
						if not paramId in dict_t2_modules[t2_module_id]:
							dict_t2_modules[t2_module_id][paramId] = []
						dict_t2_modules[t2_module_id][paramId].append(self.channel_tranflag_map[i].value)


		# Create T2 documents
		for t2_module in dict_t2_modules.keys():

			# Skip Nones
			for paramId in dict_t2_modules[t2_module].keys():
			
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
								"runState": RunStates.TO_RUN,
							},
							"$addToSet": {
								"channels": {
									"$each": dict_t2_modules[t2_module][paramId]
								}
							}
						},
						upsert=True
					)
				)

		# Insert generated t2 docs into collection
		self.logger.info("Inserting %i T2 docs into DB", len(requests))

		try: 
			self.col_t2.bulk_write(requests)
		except BulkWriteError as bwe: 
			self.logger.info(bwe.details) 
			# TODO add error flag to Job and Transient



		# Insert/Update transient document into 'transients' collection
		self.logger.info("Updating transient document")

		# TODO add alFlags
		self.col_tran.update_one(
			{"_id": tran_id},
			{
				'$addToSet': {
					'channels': {
						"$each": [
							self.channel_tranflag_map[i].value
							for i, el in enumerate(all_channels_t2_flags) 
							if el is not None
						]
					},
					'jobIds': self.job_id
				},
				"$max": {
					"lastPPDate": alert_pps_list[0]["jd"]
				}
			},
			upsert=True
		)

