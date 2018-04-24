#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/t2/T2Controller.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 25.01.2018
# Last Modified Date: 19.04.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from ampel.flags.AlDocTypes import AlDocTypes
from ampel.flags.JobFlags import JobFlags
from ampel.flags.T2RunStates import T2RunStates
from ampel.abstract.AbsT2Unit import AbsT2Unit
from ampel.pipeline.logging.LoggingUtils import LoggingUtils
from ampel.pipeline.logging.DBJobReporter import DBJobReporter
from ampel.pipeline.logging.DBLoggingHandler import DBLoggingHandler
from ampel.pipeline.db.LightCurveLoader import LightCurveLoader
from ampel.pipeline.db.DBWired import DBWired

from datetime import datetime, timedelta
from pymongo.errors import BulkWriteError
from pymongo import UpdateOne
import time, importlib, math


class T2Controller(DBWired):
	"""
	Alpha state
	"""

	version = 1.0
	_t2_units_colname = "t2_units"
	_t2_run_configs_colname = "t2_run_config"
	
	def __init__(
		self, db_host='localhost', config_db=None, base_dbs=None,
		run_state=T2RunStates.TO_RUN, t2_units=None, 
		check_interval=10, batch_size=200, conf_db_name="Ampel_config"
	): 
		"""
		mongo_client: pymongo.mongo_client.MongoClient instance pointing to the ampel database
		run_state: one on ampel.flags.T2RunStates int value (for example: T0_RUN)
		t2_units: list of string id of the t2 units to run. If not specified, any t2 unit will be run
		check_interval: int value in seconds
		batch_size: integer. It is defined because the job log entry cannot grow above 16MB (of logs).
		"""
		# TODO: add argument: max_back_time={'minutes': 0} ?

		# Get logger 
		self.logger = LoggingUtils.get_logger(unique=True)

		# Setup instance variable referencing input and output databases
		self.plug_databases(self.logger, db_host, config_db, base_dbs)
	
		# check interval is in seconds
		self.check_interval = check_interval

		# Store all known T2 units ids (name) 
		# For each T2 unit, there must exist a document with '_id' equals T2 name
		# in a collection called "t2_units" in the ampel config db
		self.unit_names = [
			el['_id'] for el in self.config_db[
				T2Controller._t2_units_colname
			].find({})
		]
		
		# Dict saving t2 classes. 
		# Key: unit name. Value: unit class
		self.t2_class = {}

		# Dict saving base config dict instances loaded from the ampel 't2_units' DB collection
		# Key: unit name, Value: dict instance
		self.t2_base_config = {}

		# Dict saving run config dict instances loaded from the ampel 't2_run_config' DB collection
		# Key: run config id(unit name + "_" + run_config name), Value: dict instance
		# Key example: POLYFIT_default
		self.t2_run_config = {}

		self.versions = {}

		# self.from_dt = datetime.utcnow() - datetime.timedelta(*backtime)

		# run_state must be an int value (see ampel.flags.T2RunStates).
		# Only the documents matching this run_state will be processed (for example: TO_RUN)
		self.run_state = run_state

		# Prepare DB query dict
		self.query = {
			"alDocType": AlDocTypes.T2RECORD,
			"runState": self.run_state.value
		}

		# list of string id of the t2 units to run. If None, any t2 unit will be run
		self.required_unit_names = t2_units

		# Update query accordingly
		if not t2_units is None:
			if len(t2_units) == 1:
				self.query['t2Unit'] = t2_units[0]
			else:
				self.query['t2Unit'] = {
					'$in': t2_units
				}


		# How many docs per 'job document'
		# batch_size is defined because the job log entry cannot grow above 16MB (of logs).
		self.batch_size = batch_size


	def start(self):
		"""
		Start monitoring the live transient database for T2 documents with the given run_state
		For now, there is no stop() method, not sure how to implement it.
		There is no return code.
		"""

		# Get handle to db collection containing transients
		tran_col = self.get_tran_col()

		while True:


			# get t2 documents (runState is usually TO_RUN or TO_RUN_PRIO)
			cursor = tran_col.find(self.query)

			# No result
			if cursor.count() == 0:
				self.logger.info("No T2 docs found")
			else:

				# Process t2_docs
				for i in range(math.ceil(cursor.count()/self.batch_size)):
					self.process_t2_docs(cursor)

			# Sleep
			time.sleep(self.check_interval)


	def process_t2_docs(self, cursor):
		"""
		cursor: mongodb cursor
		Return: nothing
		"""

		# Save current time (to approximate job duration)
		start_time = int(time.time())

		# Create JobReporter instance
		db_job_reporter = DBJobReporter(
			self.get_job_col(), JobFlags.T2
		)

		# Create new "job" document in the DB
		db_job_reporter.insert_new(
			{
				#"class": type(self).__name__,
				"class": "T2Controler",
				"version": str(T2Controller.version),
				"runState": str(self.run_state.value),
				"t2Units": str(self.required_unit_names)
			}
		)

		# Create DB logging handler instance (logging.Handler child class)
		# This class formats, saves and pushes log records into the DB
		db_logging_handler = DBLoggingHandler(
			db_job_reporter 
		)

		# Add db logging handler to stack of handlers associated with this logger
		self.logger.addHandler(db_logging_handler)
			
		# we instanciate t2 unit only once per check interval.
		# The dict t2_instances stores those instances so that these can 
		# be re-used int the while loop below
		t2_instances = {}

		# Instanciate LightCurveLoader (that returns ampel.base.LightCurve instances)
		tran_col = self.get_tran_col()
		lcl = LightCurveLoader(tran_col, self.logger)

		# Process t2_docs until next() returns None (break condition below)
		while True: 

			print("############################")
			# Retrieve newly created T2 docs
			t2_doc = next(cursor, None)

			# No new T2 doc with the given run state
			if t2_doc is None:
				return

			# Shortcut
			t2_unit_name = t2_doc['t2Unit']

			# Check if T2 instance exists in this run
			if not t2_unit_name in t2_instances:
			
				# Okay, we need to instanciate it, but do we have loaded the class already ? 
				if not t2_unit_name in self.t2_class:

					# Load class	
					self.load_unit(t2_unit_name)
				
					# add base_config version info for jobreporter
					if t2_unit_name in self.t2_base_config:
						self.add_version(
							t2_unit_name, 'base_config', self.t2_base_config[t2_unit_name]
						)

				# Instanciate T2 class
				t2_instances[t2_unit_name] = self.t2_class[t2_unit_name](
					self.logger, 
					(
						self.t2_base_config[t2_unit_name].copy()
						if t2_unit_name in self.t2_base_config 
						else None
					)
				)

				# version info of python class for jobreporter
				self.add_version(t2_unit_name, "py", t2_instances[t2_unit_name])

			# TODO: load light_curve
			# load run_config
			# run(light_curve, run_config=None)

			# Build run config id (example: )
			run_config_id = t2_unit_name + "_" + t2_doc['runConfig']

			# if run_config was not loaded not previously done
			if not run_config_id in self.t2_run_config:

				# Load it not
				self.load_run_config(run_config_id)

				# add run_config version info for jobreporter
				self.add_version(
					t2_unit_name, 'run_config', self.t2_run_config[run_config_id]
				)

			# Load ampel.base.LightCurve instance
			lc = lcl.load_from_db(
				t2_doc['tranId'], t2_doc['compoundId']
			)

			# Run t2
			before_run = datetime.utcnow().timestamp()
			ret = t2_instances[t2_unit_name].run(
				lc, (
					self.t2_run_config[run_config_id]['parameters'].copy()
					if self.t2_run_config[run_config_id] is not None
					else None
				)
			)

			# Used as timestamp and to compute duration below (using before_run)
			now = datetime.utcnow().timestamp()

			# T2 units can return a T2RunStates flag rather than a dict instance
			# for example: T2RunStates.EXCEPTION, T2RunStates.BAD_CONFIG, ...
			if isinstance(ret, T2RunStates):

				self.logger.error("T2 unit returned %s" % ret)
				# TODO: add copy t2 entry to ampel_trouble


				db_ops = [
					UpdateOne(
						{
							"_id": t2_doc['_id']
						},
						{
							'$push': {
								"results": {
									'versions': self.versions[t2_unit_name],
									'dt': now,
									'duration': before_run - now,
									'error': ret.value
								}
							},
							'$set': {
								'runState': ret.value
							}
						}
					)
				]

			else:
				self.logger.error("Saving dict returned by T2 unit")

				db_ops = [
					UpdateOne(
						{
							"_id": t2_doc['_id']
						},
						{
							'$push': {
								"results": {
									'versions': self.versions[t2_unit_name],
									'dt': now,
									'duration': round(now - before_run, 3),
									'results': ret
								}
							},
							'$set': {
								# pylint: disable=no-member
								'runState': T2RunStates.COMPLETED.value
							}
						}
					)
				]


			db_ops.append(
				UpdateOne(
					{
						"tranId": t2_doc['tranId'],
						"alDocType": AlDocTypes.TRANSIENT
					},
					{
						"$max": {
							"modified": now
						},
						"$push": {
							"lastModified": {
								'dt': now,
								'tier': 2,
								'src': t2_unit_name,
								'success': str(not isinstance(ret, T2RunStates))
							}
						}
					}
				)
			)

			try: 
				result = tran_col.bulk_write(db_ops)
				self.logger.info(result.bulk_api_result)
			except BulkWriteError as bwe: 
				# TODO add error flag to Job and Transient
				# TODO add return code 
				self.logger.error(bwe.details) 


		duration = int(time.time()) - start_time
		db_job_reporter.set_duration(duration)

		# Remove DB logging handler
		db_logging_handler.flush()
		self.logger.removeHandler(db_logging_handler)


	def load_run_config(self, run_config_id):
		"""
		run_config_id: string (unit name + "_" + run config name)
	        example: "POLYFIT_default"
		This method populates the instance variable self.t2_run_config 
		with a reference to a dict instance loaded from the database.
		(dict key is run_config_id)
		This method does not return anything.
		"""

		# Get T2 run config doc from ampel config DB
		t2_run_config_doc = next(
			self.config_db[
				T2Controller._t2_run_configs_colname
			].find(
				{'_id': run_config_id}
			),
			None
		)

		# Robustness
		if t2_run_config_doc is None:
			self.logger.info(
				"Cound not find t2 run config doc with id %s" %
				run_config_id
			)
			self.t2_run_config[run_config_id] = None
			return 


		self.t2_run_config[run_config_id] = t2_run_config_doc
		

	def load_unit(self, unit_name):
		"""	
		Loads a T2 unit class and base_config (if avail) using definitions 
		stored as document in the ampel config database.
		This method populates the instance variables self.t2_class and self.t2_base_config.
		There is no return code
		"""	

		# Robustness
		if not unit_name in self.unit_names:
			self.logger.error("Unknown T2 unit name: %s" % unit_name)
			return

		# Get T2 unit config from ampel config DB
		t2_config_doc = next(
			self.config_db[T2Controller._t2_units_colname].find(
				{'_id': unit_name}
			),
			None
		)

		# Robustness
		if t2_config_doc is None:
			self.logger.error(
				"Cound not find t2 config doc for %s in mongo config database" %
				unit_name
			)
			return 

		# Shortcut
		cfp = t2_config_doc['classFullPath'] 

		# Feedback
		self.logger.info(
			"Instanciating class '%s' using base_config version %i" %
			(cfp, t2_config_doc['version'])
		)

		# Get T2 unit class 
		module = importlib.import_module(cfp)
		t2_class = getattr(module, cfp.split(".")[-1])

		# Save *class*
		self.t2_class[unit_name] = getattr(module, cfp.split(".")[-1])

		# And save base config dict instance if avail
		if 'baseConfig' in t2_config_doc:
			self.t2_base_config[unit_name] = t2_config_doc['baseConfig'] 


	def add_version(self, unit_name, key, arg):

		if not unit_name in self.versions:
			self.versions[unit_name] = {}
		
		if type(arg) is dict:
			if arg is not None and 'version' in arg:
				self.versions[unit_name][key] = arg['version']

		elif isinstance(arg, AbsT2Unit):
			self.versions[unit_name][key] = arg.version
