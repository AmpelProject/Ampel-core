#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/t0/AlertProcessor.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 14.12.2017
# Last Modified Date: 17.03.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import time

from ampel.pipeline.t0.AmpelAlert import AmpelAlert
from ampel.pipeline.t0.AlertFileList import AlertFileList
from ampel.pipeline.t0.loaders.ZIAlertLoader import ZIAlertLoader
from ampel.pipeline.t0.ingesters.ZIAlertIngester import ZIAlertIngester

from ampel.flags.FlagGenerator import FlagGenerator
from ampel.flags.AlertFlags import AlertFlags
from ampel.flags.LogRecordFlags import LogRecordFlags
from ampel.flags.JobFlags import JobFlags

from ampel.pipeline.db.DBWired import DBWired
from ampel.pipeline.config.Channel import Channel
from ampel.pipeline.logging.LoggingUtils import LoggingUtils
from ampel.pipeline.logging.DBJobReporter import DBJobReporter
from ampel.pipeline.logging.DBLoggingHandler import DBLoggingHandler
from ampel.pipeline.logging.InitLogBuffer import InitLogBuffer

import pymongo

class AlertProcessor(DBWired):
	""" 
	Class handling T0 pipeline operations.

	For each alert, following tasks are performed:
		* Load the alert
		* Filter alert based on the configured filter
		* Set policies
		* Ingest alert based on the configured ingester
	"""
	version = 0.2

	def __init__(
		self, instrument="ZTF", alert_format="IPAC", load_channels=True,
		db_host='localhost', input_db=None, output_db=None
	):
		"""
		Parameters:
		'instrument': name of instrument (string - see set_input() docstring)
		'alert_format': format of input alerts (string - see set_input() docstring)
		'db_host': dns name or ip address (plus optinal port) of the server hosting mongod
		'load_channels': wether to load all the available channels in the config database 
		 during class instanciation or not. Dedicated can be loaded afterwards using the 
		 method load_channel(<channel name>)
		'input_db': the database containing the Ampel config collections.
		    Either:
			-> None: default settings will be used 
			   (pymongo MongoClient instance using 'db_host' and db name 'Ampel_config')
			-> string: pymongo MongoClient instance using 'db_host' 
			   and database with name identical to 'input_db' value
			-> MongoClient instance: database with name 'Ampel_config' will be loaded from 
			   the provided MongoClient instance (can originate from pymongo or mongomock)
			-> Database instance (pymongo or mongomock): provided database will be used
		'output_db': the output database (will typically contain the collections 'transients' and 'logs')
		    Either:
			-> MongoClient instance (pymongo or mongomock): the provided instance will be used 
			-> dict: (example: {'transients': 'test_transients', 'logs': 'test_logs'})
				-> must have the keys 'transients' and 'logs'
				-> values must be either string or Database instance (pymongo or mongomock)
		"""

		# Setup logger
		self.logger = LoggingUtils.get_logger(unique=True)
		self.ilb = InitLogBuffer(LogRecordFlags.T0)
		self.logger.addHandler(self.ilb)

		self.logger.info("Setting up new AlertProcessor instance")

		# Setup instance variable referencing the input database
		self.plug_databases(db_host, input_db, output_db)

		# Set static emum flag class
		Channel.set_ChannelFlags(
			# Generate ChannelFlags enum flag *class* based on DB info
			FlagGenerator.get_ChannelFlags_class(
				self.config_db['channels'],
				force_create=False
			)
		)

		# Generate T2UnitIds enum flag *class* based on DB info
		Channel.set_T2UnitIds(
			FlagGenerator.get_T2UnitIds_class(
				self.config_db['t2_units'],
				force_create=False
			)
		)

		# Setup channels
		if load_channels:
			self.load_channels()

		# Setup input type dependant parameters
		self.set_input(instrument, alert_format)

		self.logger.info("AlertProcessor initial setup completed")


	def set_input(self, instrument, alert_format):
		"""
		Depending on which instrument and institution the alerts originate,
		(as of March 2018 only ZTF & IPAC), this method performs the following:
		-> defines the alert loading function.
		-> sets required static settings in AmpelAlert
		-> instanciates the adequate ingester class
		"""

		if instrument == "ZTF":

			if alert_format == "IPAC":

				# TODO: log something ? 

				# Reference to function loading IPAC generated avro alerts
				self.alert_loading_func = ZIAlertLoader.get_flat_pps_list_from_file

				# Set static AmpelAlert alert flags
				AmpelAlert.add_class_flags(
					AlertFlags.INST_ZTF | AlertFlags.SRC_IPAC
				)

				# Set static AmpelAlert dict keywords
				AmpelAlert.set_alert_keywords(
					self.global_config['photoPoints']['ZTFIPAC']['dictKeywords']
				)
	
				# Instanciate ingester
				self.ingester = ZIAlertIngester(
					self.tran_col, self.logger
				)

				# Tell method run() that ingester method configure() must be called 
				self.setup_ingester = True

			# more alert_formats may be defined later
			else:
				raise ValueError("No implementation exists for the alert issuing entity: " + alert_format)

		# more instruments may be defined later
		else:
			raise ValueError("No implementation exists for the instrument: "+instrument)


	def load_channels(self):
		"""
		Loads all T0 channel configs defined in the T0 config 
		"""
		channel_docs = list(self.config_db['channels'].find({}))
		self.channels = [None] * len(channel_docs)

		for i, channel_doc in enumerate(channel_docs):
			self.channels[i] = Channel(
				self.config_db, db_doc=channel_doc, 
				t0_ready=True, logger=self.logger
			)

		if hasattr(self, 'ingester'):
			self.setup_ingester = True


	def load_channel(self, channel_name):
		"""
		Loads a channel config, that will be used in the method run().
		This method can be called multiple times with different channel names.
		"""
		if not hasattr(self, 'channels'):
			self.channels = []
		else:
			for channel in self.channels:
				if channel.name == channel_name:
					self.logger.info("Channel '%s' already loaded" % channel_name)
					return

		self.channels.append(
			Channel(
				self.config_db, channel_name=channel_name, 
				t0_ready=True, logger=self.logger
			)
		)

		if hasattr(self, 'ingester'):
			self.setup_ingester = True


	def reset_channels(self):
		"""
		"""
		self.channels = []
		if hasattr(self, 'ingester'):
			self.setup_ingester = True


	def set_ingester_instance(self, ingester_instance):
		"""
		Sets custom ingester instance to be used in the method run().
		If unspecified, a new instance of ZIAlertIngester() is used
		Known ingester (as for Sept 2017) are:
			* t0.ingesters.MemoryIngester
			* t0.ingesters.ZIAlertIngester
		"""
		self.ingester = ingester_instance


	def set_alert_loading_func(self, alert_loading_func):
		"""
		Sets the alert loading function.
		If the instrument is ZTF and alerts were generated by IPAC, 
		then the default alert loading function is: 
			ZIAlertLoader.get_flat_pps_list_from_file
		If you need to load ZTF IPAC alerts formatted in JSON, use:
			ap.set_alert_loading_func(
    			ZIJSONLoader.get_flat_pps_list_from_json
			)
		where ap is an instance of ALertProcessor
		"""
		self.alert_loading_func = alert_loading_func


	def run_iterable_paths(
		self, base_dir="/Users/hu/Documents/ZTF/Ampel/alerts/", 
		extension="*.avro", max_entries=None
	):
		"""
		Process alerts in a given directory (using ampel.pipeline.t0.AlertFileList)

		Parameters:
		base_dir: input directory where alerts are stored
		extension: extension of alert files (default: *.avro. Alternative: *.json)
		max_entries: limit number of files loaded 
		  max_entries=5 -> only the first 5 alerts will be processed
		
		alert files are sorted by date: sorted(..., key=os.path.getmtime)
		"""

		# Container class allowing to conveniently iterate over local avro files 
		aflist = AlertFileList(self.logger)
		aflist.set_folder(base_dir)
		aflist.set_extension(extension)

		if max_entries is not None:
			aflist.set_max_entries(max_entries)
		
		self.logger.info("Returning iterable for file paths in folder: %s" % base_dir)

		iterable = iter(
			aflist.get_files()
		)

		while iterable.__length_hint__() > 0:
			self.run(iterable)


	def run(self, iterable):
		"""
		For each alert:
			* Load the alert
			* Filter alert and set policies for every configured channels (defined by load_config())
			* Ingest alert based on PipelineIngester (default) 
			or the ingester instance set by the method set_ingester(obj)
		"""


		# Part 1: Setup logging 
		#######################

		self.logger.info("Executing run method")

		# Remove logger saving "log headers" before job(s) 
		self.logger.removeHandler(self.ilb)

		# Create JobReporter instance
		db_job_reporter = DBJobReporter(
			self.log_col, JobFlags.T0
		)

		# Create new "job" document in the DB
		db_job_reporter.insert_new(
			{
				"APVersion": str(self.version),
				"ingesterClass": str(self.ingester.__class__)
			}
		)
	
		# Create DB logging handler instance (logging.Handler child class)
		# This class formats, saves and pushes log records into the DB
		db_logging_handler = DBLoggingHandler(
			db_job_reporter, 
			previous_logs=self.ilb.get_logs()
		)

		# Add db logging handler to the logger stack of handlers 
		self.logger.addHandler(db_logging_handler)



		# Part 2: Setup divers
		######################

		self.logger.info("#######     Processing alerts     #######")

		# Save current time to later evaluate how low was the pipeline processing time
		start_time = int(time.time())


		# Check if a ingester instance was created/provided
		if not hasattr(self, 'ingester'):
			raise ValueError('Ingester instance missing.')

		if hasattr(self, 'setup_ingester'):
			self.ingester.configure(self.channels)
			del self.setup_ingester
				
		# Forward jobId to ingester instance 
		# (will be inserted in the transient documents)
		self.ingester.set_job_id(
			db_job_reporter.get_job_id()
		)

		# Array of JobFlags. Each element is set by each T0 channel 
		scheduled_t2_runnables = [None] * len(self.channels) 

		# python micro-optimization
		loginfo = self.logger.info
		logdebug = self.logger.debug
		dblh_set_tranId = db_logging_handler.set_tranId
		dblh_set_temp_flags = db_logging_handler.set_temp_flags
		dblh_unset_temp_flags = db_logging_handler.unset_temp_flags
		dblh_unset_tranId = db_logging_handler.unset_tranId
		alert_loading_func = self.alert_loading_func
		ingest = self.ingester.ingest



		# Part 3: Process alerts
		########################

		max_iter = 5000
		iter_count = 0

		# Iterate over alerts
		for element in iterable:

			iter_count += 1 

			try:

				if isinstance(element, str):
					logdebug("Processing: " + element)

				# Load avro file into python dict instance
				trans_id, pps_list = alert_loading_func(element)

				# AmpelAlert will create an immutable list of immutable pp dictionaries
				loginfo("Processing alert: " + str(trans_id))
				alert = AmpelAlert(trans_id, pps_list)

				# Associate upcoming log entries with the current transient id
				dblh_set_tranId(trans_id)

				# Loop through initialized channels
				for i, channel in enumerate(self.channels):

					# Associate upcoming log entries with the current channel
					# dblh_set_temp_flags(channel.log_flag)

					# Apply filter (returns None in case of rejection or t2 runnable ids in case of match)
					scheduled_t2_runnables[i] = channel.filter_func(alert)

					# Log feedback
					if scheduled_t2_runnables[i] is not None:
						loginfo(channel.log_accepted)
						# TODO push transient journal entry
					else:
						loginfo(channel.log_rejected)

					# Unset channel id <-> log entries association
					# dblh_unset_temp_flags(channel.log_flag)

				if not any(scheduled_t2_runnables):
					# TODO: implement AlertDisposer class ?
					self.logger.info("Disposing rejected candidates not implemented yet")

					if len(pps_list) > 1:
						# TODO check autocomplete set of ids !
						# for each channel from db_transient:
						# 		convert channel into i position
						#		scheduled_t2_runnables[i] = default_t2RunnableIds_for_this_channel
						pass
				else:
					# Ingest alert
					logdebug(" -> Ingesting alert")
					ingest(trans_id, pps_list, scheduled_t2_runnables)

				# Unset log entries association with transient id
				dblh_unset_tranId()

			except:
				self.logger.exception("")
				self.logger.critical("Exception occured")

			if iter_count > max_iter:
				self.logger.info("Reached max number of iterations")
				break


		duration = int(time.time()) - start_time
		db_job_reporter.set_duration(duration)
		self.logger.addHandler(self.ilb)
		loginfo("Alert processing completed (time required: " + str(duration) + "s)")

		# Remove DB logging handler
		db_logging_handler.flush()
		self.logger.removeHandler(db_logging_handler)
		
		return iter_count-1

def init_db():
	"""
	Initialize a MongoDB for use with Ampel
	"""
	import os, glob
	from os.path import basename, dirname
	pattern = os.path.abspath(os.path.dirname(os.path.realpath(__file__)) + '/../../../../config/hu/*/*.json')
	
	from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter
	parser = ArgumentParser(description=__doc__, formatter_class=ArgumentDefaultsHelpFormatter)
	parser.add_argument('--host', default='localhost:27017',
	    help='MongoDB server address and port')
	parser.add_argument('-d', '--database', default='Ampel',
	    help='Database name')
	parser.add_argument('--config', nargs='+', default=glob.glob(pattern),
	    help='JSON files to be inserted into the "config" collection')
	
	opts = parser.parse_args()
	
	from pymongo import MongoClient, ASCENDING
	from bson import ObjectId
	import json
	client = MongoClient(opts.host)
	
	def get_id(blob):
		if isinstance(blob['_id'], dict) and '$oid' in blob['_id']:
			return ObjectId(blob['_id']['$oid'])
		else:
			return blob['_id']
	
	db = client.get_database(opts.database)
	db['main'].create_index([('tranId', ASCENDING), ('alDocType', ASCENDING)])
	
	db = client.get_database(opts.database+'_config')
	for config in opts.config:
		collection_name = basename(dirname(config))
		collection = db[collection_name]
		with open(config) as f:
			for blob in json.load(f):
				blob['_id'] = get_id(blob)
				collection.replace_one({'_id':blob['_id']}, blob, upsert=True)

def _ingest_slice(host, infile, start, stop):
	from ampel.archive import ArchiveDB
	with open('/run/secrets/mysql-user-password') as f:
		password = f.read().strip()
	archive = ArchiveDB('postgresql://ampel:{}@localhost/ztfarchive'.format(password))
	
	def loader():
		for alert in ZIAlertLoader.walk_tarball(infile, start, stop):
			archive.insert_alert(alert, 0, 0)
			yield alert
	processor = AlertProcessor(db_host=host)
	processor.logger.setLevel('WARN')
	return processor.run(loader())

def run_alertprocessor():

	import os, time
	from concurrent import futures
	from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter
	parser = ArgumentParser(description=__doc__, formatter_class=ArgumentDefaultsHelpFormatter)
	parser.add_argument('--host', default='localhost:27017')
	parser.add_argument('--procs', type=int, default=1, help='Number of processes to start')
	parser.add_argument('--chunksize', type=int, default=50, help='Number of alerts in each process')
	
	parser.add_argument('infile')
	opts = parser.parse_args()
	
	executor = futures.ProcessPoolExecutor(opts.procs)
	
	start_time = time.time()
	step = opts.chunksize
	count = 0
	jobs = [executor.submit(_ingest_slice, opts.host, opts.infile, start, start+step) for start in range(0, opts.procs*step, step)]
	for future in futures.as_completed(jobs):
		print(future.result())
		count += future.result()
	duration = int(time.time()) - start_time
	print('Processed {} alerts in {} s ({}/s)'.format(count, duration, float(count)/duration))
	
