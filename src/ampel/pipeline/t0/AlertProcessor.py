#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/t0/AlertProcessor.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 14.12.2017
# Last Modified Date: 19.04.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import pymongo, time, numpy as np

from ampel.pipeline.t0.AmpelAlert import AmpelAlert
from ampel.pipeline.t0.AlertFileList import AlertFileList
from ampel.pipeline.t0.loaders.ZIAlertLoader import ZIAlertLoader
from ampel.pipeline.t0.ingesters.ZIAlertIngester import ZIAlertIngester

from ampel.flags.AlDocTypes import AlDocTypes
from ampel.flags.FlagGenerator import FlagGenerator
from ampel.flags.AlertFlags import AlertFlags
from ampel.flags.LogRecordFlags import LogRecordFlags
from ampel.flags.JobFlags import JobFlags

from ampel.pipeline.db.DBWired import DBWired
from ampel.pipeline.db.MongoStats import MongoStats
from ampel.pipeline.config.Channel import Channel
from ampel.pipeline.logging.LoggingUtils import LoggingUtils
from ampel.pipeline.logging.DBJobReporter import DBJobReporter
from ampel.pipeline.logging.DBLoggingHandler import DBLoggingHandler
from ampel.pipeline.logging.InitLogBuffer import InitLogBuffer


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
		db_host='localhost', config_db=None, base_dbs=None, stats=False
	):
		"""
		Parameters:
		'instrument': name of instrument (string - see set_input() docstring)
		'alert_format': format of input alerts (string - see set_input() docstring)
		'db_host': dns name or ip address (plus optinal port) of the server hosting mongod
		'load_channels': wether to load all the available channels in the config database 
		 during class instanciation or not. Dedicated can be loaded afterwards using the 
		 method load_channel(<channel name>)
		'config_db': see ampel.pipeline.db.DBWired.plug_config_db() docstring
		'base_dbs': see ampel.pipeline.db.DBWired.plug_base_dbs() docstring
		'stats': publish stats in the database (included in the job document)
		"""

		# Setup logger
		self.logger = LoggingUtils.get_logger(unique=True)
		self.ilb = InitLogBuffer(LogRecordFlags.T0)
		self.logger.addHandler(self.ilb)

		self.logger.info("Setting up new AlertProcessor instance")

		# Setup instance variable referencing the input database
		self.plug_databases(self.logger, db_host, config_db, base_dbs)

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

		# publish stats in the database (included in the job document)
		self.publish_stats = stats

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
					self.get_tran_col(), self.logger
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
		extension="*.avro", max_entries=None, console_logging=True
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
			self.run(iterable, console_logging)


	def run(self, iterable, console_logging=True):
		"""
		For each alert:
			* Load the alert
			* Filter alert and set policies for every configured channels (defined by load_config())
			* Ingest alert based on PipelineIngester (default) 
			or the ingester instance set by the method set_ingester(obj)
		"""

		# Save current time to later evaluate how low was the pipeline processing time
		time_now = time.time
		start_time = time_now()


		# Part 1: Setup logging 
		#######################

		self.logger.info("Executing run method")

		if not console_logging:
			self.logger.propagate = False

		# Remove logger saving "log headers" before job(s) 
		self.logger.removeHandler(self.ilb)

		# Create JobReporter instance
		db_job_reporter = DBJobReporter(
			self.get_job_col(), JobFlags.T0
		)

		# Create new "job" document in the DB
		db_job_reporter.insert_new(
			params = {
				"alertProc": str(self.version),
				"ingesterClass": str(self.ingester.__class__)
			},
			tier = 0
		)
	
		# Create DB logging handler instance (logging.Handler child class)
		# This class formats, saves and pushes log records into the DB
		db_logging_handler = DBLoggingHandler(
			db_job_reporter, 
			previous_logs=self.ilb.get_logs()
		)

		# Add db logging handler to the logger stack of handlers 
		self.logger.addHandler(db_logging_handler)


		# Part 2: divers
		################

		# Create array
		scheduled_t2_runnables = len(self.channels) * [None]

		# Save ampel 'state' and get list of tran ids required for autocomplete
		db_report_before, tran_ids_before = self.get_db_report()

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

		# python micro-optimization
		loginfo = self.logger.info
		logdebug = self.logger.debug
		dblh_set_tranId = db_logging_handler.set_tranId
		dblh_set_channel = db_logging_handler.set_channels
		dblh_unset_tranId = db_logging_handler.unset_tranId
		dblh_unset_channel = db_logging_handler.unset_channels
		alert_loading_func = self.alert_loading_func
		ingest = self.ingester.ingest



		# Part 3: Process alerts
		########################

		max_iter = 5000
		iter_count = 0

		st_ingest = []
		st_db_bulk = []
		st_db_op = []

		self.logger.info("#######     Processing alerts     #######")

		# Iterate over alerts
		for element in iterable:

			if iter_count == max_iter:
				self.logger.info("Reached max number of iterations")
				break

			try:

				if isinstance(element, str):
					logdebug("Processing: " + element)

				# Load avro file into python dict instance
				tran_id, pps_list = alert_loading_func(element)

				# AmpelAlert will create an immutable list of immutable pp dictionaries
				loginfo("Processing alert: " + str(tran_id))
				alert = AmpelAlert(tran_id, pps_list)

				# Associate upcoming log entries with the current transient id
				dblh_set_tranId(tran_id)

				# Loop through initialized channels
				for i, channel in enumerate(self.channels):

					# Associate upcoming log entries with the current channel
					dblh_set_channel(channel.name)

					# Apply filter (returns None in case of rejection or t2 runnable ids in case of match)
					scheduled_t2_runnables[i] = channel.filter_func(alert)

					# Log feedback
					if scheduled_t2_runnables[i] is not None:
						loginfo(channel.log_accepted)
						# TODO push transient journal entry
					else:
						
						# Autocomplete required for this channel
						if tran_ids_before[i] is not None and tran_id in tran_ids_before[i]:
							loginfo(channel.log_auto_complete)
							scheduled_t2_runnables[i] = channel.t2_flags
						else:
							loginfo(channel.log_rejected)

					# Unset channel id <-> log entries association
					dblh_unset_channel()

				if any(scheduled_t2_runnables):

					# Ingest alert
					logdebug(" -> Ingesting alert")

					start = time_now()
					#processed_alert[tran_id]
					st_db_bulk, st_db_op = ingest(tran_id, pps_list, scheduled_t2_runnables)
					st_ingest.append(time_now() - start)

				# Unset log entries association with transient id
				dblh_unset_tranId()

			except:
				self.logger.exception("")
				self.logger.critical("Exception occured")

			iter_count += 1

		# Convert python lists into numpy arrays
		st_ingest = np.array(st_ingest)
		st_db_bulk = np.array(st_db_bulk)
		st_db_op = np.array(st_db_op)

		# Save ampel 'state' and get list of tran ids required for autocomplete
		db_report_after, tran_ids_after = self.get_db_report()

		# Check post auto-complete
		for i, channel in enumerate(self.channels):
			auto_complete_diff = tran_ids_after[i] - tran_ids_before[i]
			if auto_complete_diff:
				pass

		# Add T0 job stats
		t0_stats = {
			"processed": iter_count,
			"ingested": len(st_ingest),

			# Alert ingestion: mean time & std dev in microseconds
			"ingestMean": int(round(np.mean(st_ingest)* 1000000)),
			"ingestStd": int(round(np.std(st_ingest)* 1000000)),

			# Bulk db ops: mean time & std dev in microseconds
			"dbBulkMean": int(round(np.mean(st_db_bulk)* 1000000)),
			"dbBulkStd": int(round(np.std(st_db_bulk)* 1000000)),

			# Mean single db op: mean time & std dev in microseconds
			"dbOpMean": int(round(np.mean(st_db_op)* 1000000)),
			"dbOpStd": int(round(np.std(st_db_op)* 1000000))
		}

		# Total duration in seconds
		duration = int(time_now() - start_time)

		db_job_reporter.update_job_info(
			{
				"duration": duration,
				"t0Stats": t0_stats,
				"dbStats": [db_report_before, db_report_after]
			}
		)

		# 
		self.logger.addHandler(self.ilb)

		# Restore console logging if it was removed
		if not console_logging:
			self.logger.propagate = True

		loginfo("Alert processing completed (time required: %ss)" % duration)

		# Remove DB logging handler
		db_logging_handler.flush()
		self.logger.removeHandler(db_logging_handler)
		
		# Return number of processed alerts
		return iter_count


	def get_db_report(self):
		"""
		Return values:

		First value: 'report'
		a dict instance holding various information regarding the 'state' of ampel such as:
		-> the number of documents in the DB
		-> the size of the DB
		-> the total size of all indexes
		-> the total number of transients
		-> the number of transients per channel

		Second value: 'tran_ids'
		Array - whose length equals len(self.channels) - possibly containing sets of transient ids.
		If channel[i] is the channel with index i wrt the list of channels 'self.channels', 
		and if channel[i] was configured to make use of the ampel auto_complete feature, 
		then tran_ids[i] will hold a set of transient ids listing all known 
		transients currently available in the DB for this particular channel.
		Otherwise, tran_ids_before[i] will be None.
		"""

		col = self.get_tran_col()
		tran_ids = len(self.channels) * [None]

		# Compute several values for stats
		report = {
			'dt': int(time.time()),
			# Counts total number of unique transients found in DB
			'tranNbr': col.find(
				{'alDocType': AlDocTypes.TRANSIENT}
			).count(),
			'chs': []
		}

		# Add general collection stats
		MongoStats.col_stats(col, use_dict=report)

		# Build set of transient ids
		for i, channel in enumerate(self.channels):

			if channel.get_input().auto_complete():

				# Build set of transient ids for this channel
				tran_ids[i] = {
					el['tranId'] for el in self.get_tran_col().find(
						{
							'alDocType': AlDocTypes.TRANSIENT, 
							'channels': channel.name
						},
						{
							'_id':0, 'tranId':1
						}
					)
				}

				# Update channel stats
				report['chs'].append(
					{
						'ch': channel.name, 
						'nb': len(tran_ids[i])
					}
				)

			# Update channel stats only
			else:

				report['chs'].append(
					{
						'ch': channel.name, 
						'nb': self.get_tran_col().find(
							{
								'alDocType': AlDocTypes.TRANSIENT, 
								'channels': channel.name
							}
						).count()
					}
				)

		return report, tran_ids

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
	parser.add_argument('-d', '--database', default='Ampel_config',
	    help='Configuration database name')
	parser.add_argument('--config', nargs='+', default=glob.glob(pattern),
	    help='JSON files to be inserted into the "config" collection')
	
	opts = parser.parse_args()

	dbs = create_databases(opts.host, opts.database, opts.config)
	dbs[0].add_user("ampel-readonly", read_only=True, password="password")

def create_databases(host, database_name, configs):
	
	import os
	from os.path import basename, dirname
	from pymongo import MongoClient, ASCENDING
	from bson import ObjectId
	import json
	from ampel.archive import docker_env
	client = MongoClient(host, username=os.environ.get('MONGO_INITDB_ROOT_USERNAME', 'root'), password=docker_env('MONGO_INITDB_ROOT_PASSWORD'))
	
	def get_id(blob):
		if isinstance(blob['_id'], dict) and '$oid' in blob['_id']:
			return ObjectId(blob['_id']['$oid'])
		else:
			return blob['_id']
	
	config_db = client.get_database(database_name)
	for config in configs:
		collection_name = basename(dirname(config))
		collection = config_db[collection_name]
		with open(config) as f:
			for blob in json.load(f):
				blob['_id'] = get_id(blob)
				collection.replace_one({'_id':blob['_id']}, blob, upsert=True)
	
	return client.get_database('admin'), config_db

def _ingest_slice(host, archive_host, infile, start, stop):
	from ampel.archive import ArchiveDB, docker_env
	archive = ArchiveDB('postgresql://ampel:{}@{}/ztfarchive'.format(docker_env('POSTGRES_PASSWORD'), archive_host))
	
	def loader():
		for alert in ZIAlertLoader.walk_tarball(infile, start, stop):
			archive.insert_alert(alert, 0, 0)
			yield alert
	processor = AlertProcessor(db_host=host)
	return processor.run(loader())

def _worker(idx, mongo_host, archive_host, bootstrap_host, group_id, chunk_size=5000):
	from ampel.archive import ArchiveDB, docker_env
	from ampel.pipeline.t0.ZIAlertFetcher import ZIAlertFetcher
	from pymongo import MongoClient

	archive = ArchiveDB('postgresql://ampel:{}@{}/ztfarchive'.format(docker_env('POSTGRES_PASSWORD'), archive_host))
	mongo = 'mongodb://{}:{}@{}/'.format(docker_env('MONGO_INITDB_ROOT_USERNAME'), docker_env('MONGO_INITDB_ROOT_PASSWORD'), mongo_host)

	fetcher = ZIAlertFetcher(archive, bootstrap_host, group_name=group_id)

	import time
	t0 = time.time()

	count = 0
	for i in range(10):
		processor = AlertProcessor(db_hostmongo_db, console_logging=False)
		count += processor.run(fetcher.alerts(chunk_size))
		t1 = time.time()
		dt = t1-t0
		t0 = t1
		print('({}) {} alerts in {:.1f}s; {:.1f}/s'.format(idx, chunk_size, dt, chunk_size/dt))
	return count

def run_alertprocessor():

	import os, time, uuid
	from concurrent import futures
	from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter
	parser = ArgumentParser(description=__doc__, formatter_class=ArgumentDefaultsHelpFormatter)
	parser.add_argument('--host', default='mongo:27017')
	parser.add_argument('--archive-host', default='archive:5432')
	parser.add_argument('--broker', default='epyc.astro.washington.edu:9092')
	parser.add_argument('--procs', type=int, default=1, help='Number of processes to start')
	parser.add_argument('--chunksize', type=int, default=5000, help='Number of alerts in each process')
	
	opts = parser.parse_args()
	
	executor = futures.ProcessPoolExecutor(opts.procs)

	group = uuid.uuid1()
	
	start_time = time.time()
	step = opts.chunksize
	count = 0
	jobs = [executor.submit(_worker, idx, opts.host, opts.archive_host, opts.broker, group, opts.chunksize) for idx in range(opts.procs)]
	for future in futures.as_completed(jobs):
		print(future.result())
		count += future.result()
	duration = int(time.time()) - start_time
	print('Processed {} alerts in {:.1f} s ({:.1f}/s)'.format(count, duration, float(count)/duration))
	
