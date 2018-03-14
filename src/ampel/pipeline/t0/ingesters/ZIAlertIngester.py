#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/t0/ingesters/ZIAlertIngester.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 14.12.2017
# Last Modified Date: 11.03.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import logging, pymongo
from datetime import datetime
from pymongo.errors import BulkWriteError

from ampel.abstract.AbsAlertIngester import AbsAlertIngester
from ampel.pipeline.t0.ingesters.ZIPhotoPointShaper import ZIPhotoPointShaper
from ampel.pipeline.t0.ingesters.CompoundGenerator import CompoundGenerator
from ampel.pipeline.t0.ingesters.T2MergeUtil import T2MergeUtil
from ampel.pipeline.logging.LoggingUtils import LoggingUtils

from ampel.flags.PhotoPointFlags import PhotoPointFlags
from ampel.flags.TransientFlags import TransientFlags
from ampel.flags.T2RunStates import T2RunStates
from ampel.flags.AlDocTypes import AlDocTypes
from ampel.flags.FlagUtils import FlagUtils

# https://github.com/AmpelProject/Ampel/wiki/Ampel-Flags
SUPERSEEDED = FlagUtils.get_flag_pos_in_enumflag(PhotoPointFlags.SUPERSEEDED)
TO_RUN = FlagUtils.get_flag_pos_in_enumflag(T2RunStates.TO_RUN)


class ZIAlertIngester(AbsAlertIngester):
	"""
	Ingester class used by t0.AlertProcessor in 'online' mode.
	This class 'ingests' alerts (if they have passed the alert filter):
	it compares info between alert and DB and creates several documents 
	in the DB that are used in later processing stages (T2, T3)
	"""

	version = 1.0
	new_tran_dbflag = FlagUtils.enumflag_to_dbflag(
		TransientFlags.INST_ZTF|TransientFlags.ALERT_IPAC
	)


	def __init__(self, output_collection, logger=None):
		"""
		output_collection: instance of pymongo.collection.Collection (required for database operations)
		"""
		self.logger = LoggingUtils.get_logger() if logger is None else logger
		self.col = output_collection
		self.pps_shaper = ZIPhotoPointShaper()


	def configure(self, channels):
		"""
		This function must be called before ingest(...) can be used
		channels: list of ampel.pipeline.config.Channel
		"""

		if not type(channels) is list:
			raise ValueError("Parameter channels must be of type: list")

		if len(channels) == 0:
			raise ValueError("Parameter channels cannot be empty")

		self.logger.info(
			"Configuring ZIAlertIngester with channels %s" % 
			[channel.name for channel in channels]
		)

		self.l_chanflags = [channel.get_flag() for channel in channels]

		# Static init function so that instances of CompoundGenerator work properly
		CompoundGenerator.cm_init_channels(channels)

		# instanciate T2MergeUtil (helper class used in method ingest())
		self.t2_merge_util = T2MergeUtil(channels)


	def set_job_id(self, job_id):
		"""
		An ingester class creates/updates several documents in the DB for each alert.
		Among other things, it updates the main transient document, 
		which contains a list of jobIds associated with the processing of the given transient.
		We thus need to know what is the current jobId to perform this update.
		The provided parameter should be a mongoDB ObjectId.
		"""
		self.job_id = job_id


	def set_photopoints_shaper(self, arg_pps_shaper):
		"""
		Before the ingester instance inserts new photopoints into the photopoint collection, 
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
		self.pps_shaper = arg_pps_shaper


	def get_photopoints_shaper(self):
		"""
		Get the PhotoPointShaper instance associated with this class instance.
		For more information, please check the set_photopoints_shaper docstring
		"""
		return self.pps_shaper


	def ingest(self, tran_id, pps_alert, list_of_t2_runnables):
		"""
		This method is called by t0.AmpelProcessor for 
		transients that pass at leat one T0 channel filter. 
		Then photopoints, transients and  t2 documents are pushed to the DB.
		A duplicate check is performed before DB insertions

		The function configure() must be called before this one can be used
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
		self.logger.info("Checking DB for existing pps")
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
		comp_gen = CompoundGenerator(pps_db, self.logger, ids_pps_alert)

		# python set of ids from DB photopoints 
		ids_pps_db = comp_gen.get_db_ids()

		# If no photopoint exists in the DB, then this is a new transient 
		if not ids_pps_db:
			self.logger.info("Transient is new")



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

							self.logger.info(
								"Marking photopoint %s as superseeded by %s",
								pp_db_set_superseeded["_id"], 
								pp_alert['candid']
							)

							# Update set of superseeded ids (required for t2 & compounds doc creation)
							comp_gen.add_newly_superseeded_id(
								pp_db_set_superseeded["_id"]
							)

							db_ops.append(
								pymongo.UpdateOne(
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
				self.logger.info("Transient has pps older than 30 days")



		################################################
		##   Part 3: Insert new PhotoPoints into DB   ##
		################################################

		# Difference between candids from the alert and candids present in DB 
		ids_pps_to_insert = ids_pps_alert - ids_pps_db

		# If the photopoints already exist in DB 
		if not ids_pps_to_insert:
			self.logger.info("No new photo point to insert in DB")
		else:
			self.logger.info("%i new pp(s) will be inserted into DB: %s" % (len(ids_pps_to_insert), ids_pps_to_insert))

			# Create of list photopoint dicts with photopoints matching the provided list of ids_pps_to_insert
			new_pps_dicts = [el for el in pps_alert if el['candid'] in ids_pps_to_insert]

			# ForEach 'new' photopoint (non existing in DB): 
			# Rename candid into _id, add tranId, alDocType and alFlags
			# Attention: this procedure *modifies* the dictionaries loaded by fastavro
			# (that's why you should not invert part 2 and 3 (in part 2, we access pp_alert['candid'] in
			# the case of IPAC reprocessing) unless you accept the performance penalty 
			# of copying (deep copy won't be necessary) the pp dicts from the alert)
			self.pps_shaper.ampelize(tran_id, new_pps_dicts)

			# Insert new photopoint documents into 'photopoints' collection
			for pp in new_pps_dicts:
				db_ops.append(
					pymongo.UpdateOne(
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

		chan_flags = None
		db_chan_flags = []

		for i, el in enumerate(list_of_t2_runnables):
			if not el is None:
				if chan_flags is None:
					chan_flags = self.l_chanflags[i]
				else:
					chan_flags |= self.l_chanflags[i]
				db_chan_flags.append(self.l_chanflags[i].name)

		# Generate compound info for channels having passed T0 filters (i.e being not None)
		comp_gen.generate(chan_flags)

		# See how many different compound_id were generated (possibly a single one)
		# and generate corresponding ampel document to be inserted later
		for compound_id in comp_gen.get_compound_ids(chan_flags):
		
			d_addtoset = {
				"channels": {
					"$each": [
						flag.name
						for flag in comp_gen.get_channels_for_compoundid(compound_id).as_list()
					]
				}
			}

			if comp_gen.has_flavors(compound_id):
				d_addtoset["flavors"] = {
					"$each": comp_gen.get_compound_flavors(compound_id) # returns a list
				}
			
			pps_dict = comp_gen.get_eff_compound(compound_id)
			db_ops.append(
				pymongo.UpdateOne(
					{
						"_id": compound_id, 
					},
					{
						"$setOnInsert": {
							"_id": compound_id,
							"tranId": tran_id,
							"alDocType": AlDocTypes.COMPOUND,
							"tier": 0,
							"added": datetime.today().timestamp(),
							"lastppdt": pps_alert[0]['jd'],
							"len": len(pps_dict),
							"pps": pps_dict
						},
						"$addToSet": d_addtoset
					},
					upsert=True
				)
			)
			


		#####################################
		##   Part 5: Generate t2 documents ##
		#####################################

		self.logger.debug("Generating T2 docs")
		ddd_t2_struct = self.t2_merge_util.get_struct(
			comp_gen, list_of_t2_runnables
		)
		
		# counter for user feedback (after next loop)
		db_ops_len = len(db_ops)

		# Loop over t2 runnables
		for t2_id in ddd_t2_struct.keys():

			# Loop over run settings
			for run_config in ddd_t2_struct[t2_id].keys():
			
				# Loop over compound Ids
				for compound_id in ddd_t2_struct[t2_id][run_config]:

					d_addtoset = {
						"channels": {
							"$each": [
								el.name for el in ddd_t2_struct[t2_id][run_config][compound_id].as_list()
							]
						}
					}

					if comp_gen.has_flavors(compound_id):
						d_addtoset["flavors"] = {
							"$each": comp_gen.get_t2_flavors(compound_id) # returns a list
						}

					db_ops.append(
						pymongo.UpdateOne(
							{
								"tranId": tran_id, 
								"t2Unit": t2_id.name, 
								"runConfig": run_config, 
								"compoundId": compound_id,
							},
							{
								"$setOnInsert": {
									"tranId": tran_id,
									"alDocType": AlDocTypes.T2RECORD,
									"t2Unit": t2_id.name, 
									"runConfig": run_config, 
									"compoundId": compound_id, 
									"runState": TO_RUN,
								},
								"$addToSet": d_addtoset
							},
							upsert=True
						)
					)

		# Insert generated t2 docs into collection
		self.logger.info("%i T2 docs will be inserted into DB", len(db_ops) - db_ops_len)



		############################################
		##   Part 6: Update transient documents   ##
		############################################

		# Insert/Update transient document into 'transients' collection
		self.logger.info("Updating transient document")

		now = datetime.today().timestamp()

		# TODO add alFlags
		db_ops.append(
			pymongo.UpdateOne(
				{
					"tranId": tran_id,
					"alDocType": AlDocTypes.TRANSIENT
				},
				{
					"$setOnInsert": {
						"tranId": tran_id,
						"alDocType": AlDocTypes.TRANSIENT
					},
					'$addToSet': {
						"alFlags": {
							"$each": ZIAlertIngester.new_tran_dbflag
						},
						'channels': {
							"$each": db_chan_flags
						},
						'jobIds': self.job_id,
						'modified': now
					},
					"$max": {
						"lastPPDate": pps_alert[0]["jd"]
					},
					"$push": {
						"lastModified": {
							'dt': now,
							'tier': 0,
							'src': "ZI"
						}
					}
				},
				upsert=True
			)
		)

		try: 
			result = self.col.bulk_write(db_ops)
			self.logger.info(result.bulk_api_result)
		except BulkWriteError as bwe: 
			self.logger.info(bwe.details) 
			# TODO add error flag to Job and Transient
			# TODO add return code 
