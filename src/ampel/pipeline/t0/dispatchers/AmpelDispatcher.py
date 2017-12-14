#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : /Users/hu/Documents/ZTF/Ampel/src/ampel/pipeline/t0/dispatchers/AmpelDispatcher.py
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 14.12.2017
# Last Modified Date: 14.12.2017
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>
import logging, importlib, hashlib
from pymongo import UpdateOne, InsertOne
from pymongo.errors import BulkWriteError
from ampel.pipeline.t0.dispatchers.AbtractTransientsDispatcher import AbtractTransientsDispatcher
from ampel.pipeline.common.flags.T2SchedulingFlags import T2SchedulingFlags
from ampel.pipeline.common.flags.PhotoPointFlags import PhotoPointFlags
from ampel.pipeline.common.flags.TransientFlags import TransientFlags
from ampel.pipeline.common.flags.RunStates import RunStates

class AmpelDispatcher(AbtractTransientsDispatcher):
	"""
		Dispatcher class used by t0.AlertProcessor in 'online' mode.
		This class re-route transient candidates into the NoSQL database
		if they have passed the configured filter.
	"""

	def __init__(self, mongo_client, dispatching_what="ZTFIPAC"):
		"""
			The parameter mongo_client (instance of pymongo.MongoClient) is required for database operations.
			Transient info will be stored the collection 'incoming' from the 'T0' database
			Arguments:
				dispatching_what: A string made of an instrument description (ex: ZTF) 
				and photopoints source (ex: IPAC). Example: ZTFIPAC, ZTFNUGENS, ...
		"""
		self.logger = logging.getLogger("Ampel")
		self.set_mongo(mongo_client)

		db_conf = next(self.db["config"].find({}))
		ppts_conf = db_conf["global"]["photoPoints"][dispatching_what]
		self.set_pp_dict_keywords(ppts_conf['dictKeywords'])

		module = importlib.import_module("ampel.pipeline.t0.stampers." + ppts_conf['stamperClass'])
		self.pps_stamper = getattr(module, ppts_conf['stamperClass'])()

		for flag in ppts_conf["alFlags"].split("|"):
			self.pps_stamper.append_base_flags(PhotoPointFlags[flag])

		self.t2s_params_channels = {}

		# Loop through schedulable t2 modules 
		for t2_module_sf in T2SchedulingFlags:

			# Each entry in the t2s_params_channels dict is also a dict 
			# (with the different possible paramIds as key)
			# 1st key: scheduling flag  2nd key: paramId 
			self.t2s_params_channels[t2_module_sf] = {}

			# Loop through the t0 channels (TODO: provide list of *used* T0 channels 
			# rather than looping through everything)
			for t0_channel in [keys for keys in db_conf["T0"]["channels"]]:

				# Extract the paramerter ID associated with the t2_module 
				paramId = None
				for t0_t2_module in db_conf["T0"]["channels"][t0_channel]["t2Modules"]:
					if t0_t2_module["module"] == t2_module_sf.name:
						paramId = t0_t2_module["paramId"]

				# if paramId was not found, it means the current t0_channel 
				# has not registered the current t2_module
				if paramId is None:
					continue
				
				if not paramId in self.t2s_params_channels[t2_module_sf]:
					self.t2s_params_channels[t2_module_sf][paramId] = TransientFlags(0)

				self.t2s_params_channels[t2_module_sf][paramId] |= TransientFlags[
					db_conf["T0"]["channels"][t0_channel]['flagLabel']
				]


	def set_jobId(self, jobId):
		self.jobId = jobId


	def set_photopoints_stamper(self, arg_pps_stamper):
		self.pps_stamper = arg_pps_stamper


	def get_photopoints_stamper(self):
		return self.pps_stamper


	def map_channel_to_transient_flag(self, transient_flag_list):
		self.channel_tranflag_map = transient_flag_list


	def set_pp_dict_keywords(self, keywords):
		"""
		"""
		self.tran_id_kw = keywords["tranId"]
		self.ppt_id_kw = keywords["pptId"]
		self.obs_date_kw = keywords["obsDate"]
		self.filter_id_kw = keywords["filterId"]


	def set_mongo(self, mongo_client):
		"""
			Sets the mongo client (instance of pymongo.MongoClient) for database operations.
			Transient info will be stored the collection 'incoming' from the 'T0' database
		"""
		self.db = mongo_client["Ampel"]
		self.col_pps = self.db["photopoints"]
		self.col_tran = self.db["transients"]
		self.col_t2 = self.db["t2"]



	def dispatch(self, tran_id, alert_pps_list, all_channels_t2_flags, force=False):
		"""
			This method is called by t0.AmpelProcessor for 
			transients that passe at leat one T0 channel filter. 
			Then photopoints, transients and  t2 documents are pushed to the DB.
			A duplicate check is performed before DB insertions
		"""

		# TODO remove this when going to production
		alert_pps_list = [el for el in alert_pps_list if 'candid' in el and el['candid'] is not None]

		# micro optimization: local variable shortcut
		ppt_id_kw = self.ppt_id_kw

		# All candids from the alert
		ppt_ids_in_alert = {el[ppt_id_kw] for el in alert_pps_list}

		# Check existing photopoints in DB
		self.logger.info("Checking DB for existing ppts")
		db_ppts_lookup = self.col_pps.find(
			{"tranId": tran_id}, 
			{"_id": 1, "alFlags": 1}
		)

		ppt_ids_in_db = set()
		excluded_ppt_ids = set()
		wzm_ids = set()

		# If no photopoint exists in the DB, then this is a new transient 
		if db_ppts_lookup.count() == 0:
			ppt_ids_to_insert = ppt_ids_in_alert
			self.logger.info("Transient is new")
		else:

			for db_ppt in db_ppts_lookup:

				ppt_ids_in_db.add(db_ppt["_id"])
				alFlags = PhotoPointFlags(db_ppt["alFlags"])

				if PhotoPointFlags.PP_EXCLUDE in alFlags:
					self.logger.info("Following PPT is marked for exclusion: %s", db_ppt["_id"])
					excluded_ppt_ids.add(db_ppt["_id"])
				if PhotoPointFlags.HAS_WEIZMANN_PHOTO in alFlags:
					self.logger.info("Follwing PPT has WZM photometry: %s", db_ppt["_id"])
					wzm_ids.add(db_ppt["_id"])

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

			# If set db_ppt_ids_not_in_alert not empty, ppts reprocessing occured at IPAC
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
					for alert_ppt in alert_pps_list:
						if (
							superseeded_db_ppt["jd"] == alert_ppt["jd"] and
							superseeded_db_ppt["pid"] == alert_ppt["pid"] and
							superseeded_db_ppt["fid"] == alert_ppt["fid"] 
						):

							self.logger.info(
								"Marking ppt %s as superseeded by %s",
								superseeded_db_ppt["_id"], 
								alert_ppt['candid']
							)

							# Update set of excluded ids (will be used when creating t2 docs)
							excluded_ppt_ids.add(superseeded_db_ppt["_id"])

							requests.append(
								UpdateOne(
									{'_id': superseeded_db_ppt["_id"]}, 
									{
										'$addToSet': {
											'newid': alert_ppt['candid'],
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
		new_ppts_dicts = [el for el in alert_pps_list if el[ppt_id_kw] in ppt_ids_to_insert]

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
		if db_ppts_lookup.count() == 0:
			for ppt_id in sorted(ppt_ids_to_insert):
				hash_payload += '%i' % ppt_id
				compound.append({'ppt': ppt_id})
		else:
			for ppt_id in sorted(set().union(ppt_ids_in_db, ppt_ids_in_alert) - excluded_ppt_ids):
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

			# loop through all possible scheduling flags 
			for t2_module_sf in T2SchedulingFlags:

				# Ignore scheduling flags not set for this channel
				if not t2_module_sf in single_channel_t2_flags:
					continue
				
				if not t2_module_sf in dict_t2_modules:
					dict_t2_modules[t2_module_sf] = {}

				# loop through all known paramIds for this t2 module
				for paramId in self.t2s_params_channels[t2_module_sf].keys():
				
					# If the transientFlag of the current channel (index i)
					# is registered in t2s_params_channels
					if self.channel_tranflag_map[i] in self.t2s_params_channels[t2_module_sf][paramId]:
						if not paramId in dict_t2_modules[t2_module_sf]:
							dict_t2_modules[t2_module_sf][paramId] = []
						dict_t2_modules[t2_module_sf][paramId].append(self.channel_tranflag_map[i].value)


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
					'jobIds': self.jobId
				},
				"$max": { 
					"lastPPDate": alert_pps_list[0]["jd"]
				}
			},
			upsert=True
		)

