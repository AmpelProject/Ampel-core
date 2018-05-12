#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/t0/ingesters/ZIAlertIngester.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 14.12.2017
# Last Modified Date: 05.05.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import logging, pymongo, time
from datetime import datetime, timezone
from pymongo.errors import BulkWriteError

from ampel.abstract.AbsAlertIngester import AbsAlertIngester
from ampel.pipeline.t0.ingesters.ZIPhotoDictShaper import ZIPhotoDictShaper
from ampel.pipeline.t0.ingesters.CompoundBluePrint import CompoundBluePrint
from ampel.pipeline.t0.ingesters.T2DocsBluePrint import T2DocsBluePrint
from ampel.pipeline.t0.ingesters.ZICompElement import ZICompElement
from ampel.pipeline.logging.LoggingUtils import LoggingUtils

from ampel.flags.PhotoFlags import PhotoFlags
from ampel.flags.TransientFlags import TransientFlags
from ampel.flags.T2RunStates import T2RunStates
from ampel.flags.AlDocTypes import AlDocTypes
from ampel.flags.FlagUtils import FlagUtils

from functools import reduce
from operator import or_

# https://github.com/AmpelProject/Ampel/wiki/Ampel-Flags
SUPERSEEDED = FlagUtils.get_flag_pos_in_enumflag(PhotoFlags.SUPERSEEDED)
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


	def __init__(
		self, channels, t2_units_col, output_col, logger=None,
		check_reprocessing=True, alert_history_length=30
	):
		"""
		channels: list of ampel.pipeline.config.Channel
		t2_units_col: the db collection from the config DB hosting t2 unit parameters
		output_col: instance of pymongo.collection.Collection (required for database operations)
		"""

		if not type(channels) is list:
			raise ValueError("Parameter channels must be of type: list")

		if len(channels) == 0:
			raise ValueError("Parameter channels cannot be empty")

		self.channel_names = tuple(channel.name for channel in channels)
		self.logger = LoggingUtils.get_logger() if logger is None else logger
		self.logger.info("Configuring ZIAlertIngester for channels %s" % repr(self.channel_names))
		
		# T2 unit making use of upper limits
		self.t2_units_using_uls = tuple(
			el["_id"] for el in t2_units_col.find({}) if el['upperLimits'] is True
		)

		# instantiate util classes used in method ingest()
		self.photo_shaper = ZIPhotoDictShaper()
		self.t2_blueprint_creator = T2DocsBluePrint(channels, self.t2_units_using_uls)
		self.comp_gen = CompoundBluePrint(
			ZICompElement(channels), self.logger
		)

		self.logger.info(
			"CompoundBluePrint instantiated using ZICompElement version %0.1f" % 
			ZICompElement.version
		)

		self.col = output_col
		self.add_dbbulk_stat = self.add_dbop_stat = None
		self.check_reproc = check_reprocessing
		self.al_hist_len = alert_history_length

		self.lookup_projection = {
			"_id": 1,
			"alFlags": 1,
			"jd": 1,
			"fid": 1,
			"pid": 1,
			"alExcluded": 1
		}


	def flush_report(self):
		pass


	def set_job_id(self, job_id):
		"""
		An ingester class creates/updates several documents in the DB for each alert.
		Among other things, it updates the main transient document, 
		which contains a list of jobIds associated with the processing of the given transient.
		We thus need to know what is the current jobId to perform this update.
		The provided parameter should be a mongoDB ObjectId.
		"""
		self.job_id = job_id


	def set_stats_dict(self, stats_dict):
		"""
		"""

		if not 'dbBulkTime' in stats_dict:
			stats_dict['dbBulkTime'] = []

		if not 'dbOpTime' in stats_dict:
			stats_dict['dbOpTime'] = []

		self.add_dbbulk_stat = stats_dict['dbBulkTime'].append
		self.add_dbop_stat = stats_dict['dbOpTime'].append


	def set_photodict_shaper(self, arg_photo_shaper):
		"""
		Before the ingester instance inserts new photopoints or upper limits into the database, 
		it 'customizes' (or 'ampelizes' if you will) them in order to later enable
		the use of short and flexible queries. 
		The cutomizations are minimal, most of the original structure is kept.
		For exmample, in the case of ZIPhotoDictShaper:
			* The field candid is renamed in _id 
			* A new field 'alFlags' (AmpelFlags) is created (integer value of ampel.flags.PhotoFlags)
			* A new field 'alDocType' is created (integer value of ampel.flags.AlDocTypes.PHOTOPOINT or UPPERLIMIT)
		A photopoint shaper class (t0.pipeline.ingesters...) performs these operations.
		This method enables the customization of the PhotoDictShaper instance to be used.
		By default, ZIPhotoDictStamper is used.
		"""
		self.photo_shaper = arg_photo_shaper


	def get_photodict_shaper(self):
		"""
		Get the PhotoDictShaper instance associated with this class instance.
		For more information, please check the set_photodict_shaper docstring
		"""
		return self.photo_shaper


	def ingest(self, tran_id, pps_alert, uls_alert, list_of_t2_units):
		"""
		This method is called by t0.AmpelProcessor for alerts passing at least one T0 channel filter. 
		Photopoints, transients and  t2 documents are created and saved into the DB.
		Note: Some dict instances referenced in pps_alert and uls_alert might be modified by this method.
		"""

		###############################################
		##   Part 1: Gather info from DB and alert   ##
		###############################################

		db_ops = []

		# Load existing photopoint and upper limits from DB if any
		self.logger.info("Checking DB for existing pps/uls")
		meas_db = self.col.find(
			{
				# tranId should be specific to one instrument
				"tranId": tran_id,
				"alDocType": {
					'$in': [
						AlDocTypes.PHOTOPOINT, 
						AlDocTypes.UPPERLIMIT
					]
				}
			},
			self.lookup_projection
		)

		pps_db = []
		uls_db = []
		a1 = pps_db.append
		a2 = uls_db.append

		# Create pps / uls lists from (mixed) db results
		for el in meas_db:
			if 'magpsf' in el:  
				a1(el) # Photopoint
			else:
				a2(el) # Upper limit

		# Default refs to empty list (list concatenation occurs later)
		pps_to_insert = uls_to_insert = []

		# Create set with pp ids from alert
		ids_pps_alert = {pp['candid'] for pp in pps_alert}

		# python set of ids of photopoints from DB
		ids_pps_db = {el['_id'] for el in pps_db}

		# Create unique ids for the upper limits from alert
		# Example using the following upper limit: 
		# {
		#   'diffmaglim': 19.024799346923828,
 		#   'fid': 2,
 		#   'jd': 2458089.7405324,
 		#   'pdiffimfilename': '/ztf/archive/sci/2017/1202/240532/ \
		#                      ztf_20171202240532_000566_zr_c08_o_q1_scimrefdiffimg.fits.fz',
 		#   'pid': 335240532815,
 		#   'programid': 0
		# }
		# -> generated ID: 2458089740532428190247993
		# -> %timeit: 1,3 microsecond on MBP 15" 2017
		ids_uls_alert = set()
		
		# Process upper limits if provided
		if uls_alert is not None:

			JD2017 = 2457754.5

			for ul in uls_alert:

				# extract quadrant number from pid (not avail as dedicate key/val)
				ul['rcid'] = str(ul['pid'])[8:10]

				# Update avro dict
				ul['_id'] = int(
					"%i%s%i" % (
						# Convert jd float into int by multiplying it by 10**6
						# we thereby drop the last digit (milisecond) which is pointless
						# for the present purpose
						(JD2017 - ul['jd']) * 1000000, 
						ul['rcid'], 
						ul['diffmaglim'] * 1000  # cut of mag float after 3 digits after coma
					)
				)

				# Build list ? then cast to set. Use list later in ampelize to create dict
				ids_uls_alert.add(ul['_id'])

		# python set of ids of upper limits from DB
		ids_uls_db = {el['_id'] for el in uls_db}

		# If no photopoint exists in the DB, then this is a new transient 
		if not ids_pps_db:
			self.logger.info("Transient is new")



		#################################################################
		##   Part 2: Insert new photopoints and upper limits into DB   ##
		#################################################################

		# Difference between candids from the alert and candids present in DB 
		ids_pps_to_insert = ids_pps_alert - ids_pps_db
		ids_uls_to_insert = ids_uls_alert - ids_uls_db

		# If the photopoints already exist in DB 
		#self._insert_feedback(self, ids_pps_to_insert, "photo point")
		#self._insert_feedback(self, ids_uls_to_insert, "point")

		# PHOTO POINTS
		if ids_pps_to_insert:

			self.logger.info(
				"%i new photo point(s) will be inserted into DB: %s" % 
				(len(ids_pps_to_insert), ids_pps_to_insert)
			)

			# ForEach photopoint not existing in DB: 
			# Rename candid into _id, add tranId, alDocType (PHOTOPOINT) and alFlags
			# Attention: ampelize *modifies* dict instances loaded by fastavro
			pps_to_insert = self.photo_shaper.ampelize(
				tran_id, pps_alert, ids_pps_to_insert
			)

			for pp in pps_to_insert:
				db_ops.append(
					pymongo.UpdateOne(
						{"_id": pp["_id"]},
						{"$setOnInsert": pp},
						upsert=True
					)
				)
		else:
			self.logger.info("No new photo point to insert into DB")

		# UPPER LIMITS
		if ids_uls_to_insert:

			self.logger.info(
				"%i new upper limit(s) will be inserted into DB: %s" % 
				(len(ids_uls_to_insert), ids_uls_to_insert)
			)

			# For each upper limit not existing in DB: 
			# Add tranId, alDocType (UPPER_LIMIT) and alFlags
			# Attention: ampelize *modifies* dict instances loaded by fastavro
			uls_to_insert = self.photo_shaper.ampelize(
				tran_id, uls_alert, ids_uls_to_insert, id_field_name='_id'
			)

			# Insert new upper limit into DB
			for ul in uls_to_insert:
				db_ops.append(
					pymongo.UpdateOne(
						{"_id": ul["_id"]},
						{
							"$setOnInsert": ul,
							"$addToSet": {
								'tranId': tran_id
							}
						},
						upsert=True
					)
				)
		else:
			self.logger.info("No new upper limit to insert into DB")




		###################################################
		##   Part 3: Check for reprocessed photopoints   ##
		###################################################

		# NOTE: this procedure will *update* selected the dict instances 
		# loaded from DB (from the lists: pps_db and uls_db)

		# Difference between candids from db and candids from alert
		ids_in_db_not_in_alert = (ids_pps_db | ids_uls_db) - (ids_pps_alert | ids_uls_alert)

		# If the set is not empty, either some transient info is older that al_hist_len days
		# or some photopoints were reprocessed
		if self.check_reproc and ids_in_db_not_in_alert:

			# Ignore ppts in db older than al_hist_len days  
			min_jd = pps_alert[0]["jd"] - self.al_hist_len
			ids_in_db_older_than_xx_days = {el["_id"] for el in pps_db + uls_db if el["jd"] < min_jd}
			ids_superseeded = ids_in_db_not_in_alert - ids_in_db_older_than_xx_days

			# pps/uls reprocessing occured at IPAC
			if ids_superseeded:

				# loop through superseeded photopoint
				for photod_db_superseeded in filter(
					lambda x: x['_id'] in ids_superseeded, pps_db + uls_db
				):

					# Match these with new alert data (already 'shaped' by the ampelize method)
					for new_meas in filter(lambda x: 
						# jd alone is actually enough for matching pps reproc 
						x['jd'] == photod_db_superseeded['jd'] and 
						x['rcid'] == photod_db_superseeded['rcid'], 
						pps_to_insert + uls_to_insert
					):

						self.logger.info(
							"Marking measurement %s as superseeded by %s",
							photod_db_superseeded["_id"], 
							new_meas['_id']
						)

						# Update flags in dict loaded by fastavro
						# (required for t2 & compounds doc creation)
						if SUPERSEEDED not in photod_db_superseeded['alFlags']:
							photod_db_superseeded['alFlags'].append(SUPERSEEDED)

						# Create and append pymongo update operation
						db_ops.append(
							pymongo.UpdateOne(
								{'_id': photod_db_superseeded["_id"]}, 
								{
									'$addToSet': {
										'newId': new_meas['_id'],
										'alFlags': SUPERSEEDED
									}
								}
							)
						)
			else:
				self.logger.info("Transient data older than 30 days exist in DB")




		#####################################################
		##   Part 4: Generate compound ids and compounds   ##
		#####################################################

		# Generate tuple of channel names
		chan_names = tuple(
			chan_name for chan_name, t2_units in zip(self.channel_names, list_of_t2_units) 
			if t2_units is not None
		)

		# Compute compound ids (used later for creating compounds and t2 docs)
		comp_gen = self.comp_gen
		self.comp_gen.generate(
			sorted(
				pps_db + pps_to_insert + uls_db + uls_to_insert, 
				key=lambda k: k['jd']
			),
			# Do computation only for chans having passed T0 filters (not None)
			chan_names
		)

		# See how many different eff_comp_id were generated (possibly a single one)
		# and generate corresponding ampel document to be inserted later
		for eff_comp_id in comp_gen.get_effids_of_chans(chan_names):
		
			d_addtoset = {
				"channels": {
					"$each": list(
						comp_gen.get_chans_with_effid(eff_comp_id)
					)
				}
			}

			if comp_gen.has_flavors(eff_comp_id):
				d_addtoset["flavors"] = {
					# returns tuple
					"$each": comp_gen.get_compound_flavors(eff_comp_id)
				}
			
			comp_dict = comp_gen.get_eff_compound(eff_comp_id)
			pp_comp_id = comp_gen.get_ppid_of_effid(eff_comp_id)

			d_set_on_insert =  {
				"_id": eff_comp_id,
				"tranId": tran_id,
				"alDocType": AlDocTypes.COMPOUND,
				"tier": 0,
				"added": datetime.utcnow().timestamp(),
				"lastppdt": pps_alert[0]['jd'],
				"len": len(comp_dict),
				"comp": comp_dict
			}

			if pp_comp_id != eff_comp_id:
				d_set_on_insert['ppCompId'] = pp_comp_id

			db_ops.append(
				pymongo.UpdateOne(
					{"_id": eff_comp_id},
					{
						"$setOnInsert": d_set_on_insert,
						"$addToSet": d_addtoset
					},
					upsert=True
				)
			)
			


		#####################################
		##   Part 5: Generate t2 documents ##
		#####################################

		self.logger.debug("Generating T2 docs")
		t2docs_blueprint = self.t2_blueprint_creator.create_blueprint(
			comp_gen, list_of_t2_units
		)
		
		# counter for user feedback (after next loop)
		db_ops_len_before = len(db_ops)
		now = datetime.now(timezone.utc).timestamp()

		# Loop over t2 runnables
		for t2_id in t2docs_blueprint.keys():

			# Loop over run settings
			for run_config in t2docs_blueprint[t2_id].keys():
			
				# Loop over compound Ids
				for bifold_comp_id in t2docs_blueprint[t2_id][run_config]:

					# Set of channel names
					eff_chan_names = list(
						t2docs_blueprint[t2_id][run_config][bifold_comp_id]
					)

					journal = []

					# Matching search criteria
					match_dict = {
						"tranId": tran_id,
						"alDocType": AlDocTypes.T2RECORD,
						"t2Unit": t2_id, 
						"runConfig": run_config
						#"compoundId": bifold_comp_id,
					}

					# Attributes set if no previous doc exists
					d_set_on_insert = {
						"tranId": tran_id,
						"alDocType": AlDocTypes.T2RECORD,
						"t2Unit": t2_id, 
						"runConfig": run_config, 
						"runState": TO_RUN
					}

					# Update set of channels
					d_addtoset = {
						"channels": {
							"$each": eff_chan_names
						}
					}

					# T2 doc referencing multiple compound ids (== T2 ignoring upper limits)
					# bifold_comp_id is then a pp_compound_id
					if t2_id not in self.t2_units_using_uls:

						# match_dict["compoundId"] = bifold_comp_id or 
						# match_dict["compoundId"] = {"$in": [bifold_comp_id]}
						# triggers the error: "Cannot apply $addToSet to non-array field. \
						# Field named 'compoundId' has non-array type string"
						# -> See https://jira.mongodb.org/browse/SERVER-3946
						match_dict["compoundId"] = {
							"$elemMatch": {
								"$eq": bifold_comp_id
							}
						}

						d_addtoset["compoundId"] = {
							"$each": list(
								{bifold_comp_id} | 
								comp_gen.get_effids_of_chans(eff_chan_names)
							)
						}

						# Update journal: register eff id for each channel
						journal_entries = [
							{
								"dt": now,
								"chan": chan_name,
								"effId": comp_gen.get_effid_of_chan(chan_name),
								"op": "addToSet"
							}
							for chan_name in eff_chan_names
						]

						# Update journal: register pp id common to all channels
						journal_entries.insert(0, 
							{
								"dt": now,
								"channels": eff_chan_names,
								"ppId": bifold_comp_id,
								"op": "upsertPp"
							}
						)

						# Update journal
						d_addtoset["journal"] = {"$each": journal_entries}

					# T2 doc referencing a single compound id
					# bifold_comp_id is then an eff_compound_id
					else:

						match_dict["compoundId"] = bifold_comp_id
						d_set_on_insert["compoundId"] = bifold_comp_id

						# Update journal
						d_addtoset["journal"] = {
							"dt": now,
							"channels": eff_chan_names,
							"op": "upsertEff"
						}


					# Append update operation to bulk list
					db_ops.append(
						pymongo.UpdateOne(
							match_dict,
							{
								"$setOnInsert": d_set_on_insert,
								"$addToSet": d_addtoset
							},
							upsert=True
						)
					)

		# Insert generated t2 docs into collection
		self.logger.info("%i T2 docs will be inserted into DB", len(db_ops) - db_ops_len_before)



		############################################
		##   Part 6: Update transient documents   ##
		############################################

		# Insert/Update transient document into 'transients' collection
		self.logger.info("Updating transient document")

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
							"$each": chan_names
						},
						'jobIds': self.job_id,
					},
					"$max": {
						"lastPPDate": pps_alert[0]["jd"],
						"modified": now
					},
					"$push": {
						"lastModified": {
							'dt': now,
							'tier': 0,
							'src': "ZIAI", # ZIAlertIngesert
							'channels': chan_names
						}
					}
				},
				upsert=True
			)
		)

		try: 

			if self.add_dbbulk_stat is not None:

				start = time.time()
				# print("#######")
				# print(db_ops)
				# print("#######")
				db_op_results = self.col.bulk_write(db_ops) # DB update
				time_delta = time.time() - start
				self.add_dbbulk_stat(time_delta)
				self.add_dbop_stat(time_delta / len(db_ops))

			else:
				db_op_results = self.col.bulk_write(db_ops) # DB update

			# Feedback
			if (
				len(db_op_results.bulk_api_result['writeErrors']) > 0 or
				len(db_op_results.bulk_api_result['writeConcernErrors']) > 0
			):
				self.logger.error(db_op_results.bulk_api_result)
			else:
				self.logger.info(
					"DB feeback: %i upserted" % 
					db_op_results.bulk_api_result['nUpserted']
				)

		except BulkWriteError as bwe: 
			self.logger.info(bwe.details) 
			# TODO add error flag to Job and Transient
			# TODO add return code 



	def _insert_feedback(self, ids_to_insert, measurement):

		# If the photopoints already exist in DB 
		if not ids_to_insert:
			self.logger.info(
				"No new %s to insert in DB" % measurement
			)
		else:
			self.logger.info(
				"%i new %s(s) will be inserted into DB: %s" % 
				(len(ids_to_insert), measurement, ids_to_insert)
			)
