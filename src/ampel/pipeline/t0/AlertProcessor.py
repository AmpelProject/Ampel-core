#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/t0/AlertProcessor.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 10.10.2017
# Last Modified Date: 14.06.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import pymongo, time, numpy as np
from ampel.pipeline.t0.alerts.AlertSupplier import AlertSupplier
from ampel.pipeline.t0.alerts.ZIAlertShaper import ZIAlertShaper
from ampel.pipeline.t0.ingesters.ZIAlertIngester import ZIAlertIngester
from ampel.pipeline.logging.LoggingUtils import LoggingUtils
from ampel.pipeline.logging.DBJobReporter import DBJobReporter
from ampel.pipeline.logging.DBLoggingHandler import DBLoggingHandler
from ampel.pipeline.logging.InitLogBuffer import InitLogBuffer
from ampel.pipeline.db.DBWired import DBWired
from ampel.pipeline.common.GraphiteFeeder import GraphiteFeeder
from ampel.pipeline.config.ChannelLoader import ChannelLoader
from ampel.base.AmpelAlert import AmpelAlert
from ampel.flags.AlDocTypes import AlDocTypes
from ampel.flags.AlertFlags import AlertFlags

class AlertProcessor(DBWired):
	""" 
	Class handling T0 pipeline operations.

	For each alert, following tasks are performed:
		* Load the alert
		* Filter alert based on the configured filter
		* Set policies
		* Ingest alert based on the configured ingester
	"""
	version = 0.5
	iter_max = 5000

	def __init__(
		self, channels=None, source="ZTFIPAC", mongodb_uri='localhost', 
		config=None, central_db=None, publish_stats=['graphite', 'jobs'], 
		load_ingester=True
	):
		"""
		Parameters:
		-----------
		'channels': 
		   - None: all the available channels from the ampel config will be loaded
		   - String: channel with the provided id will be loaded
		   - List of strings: channels with the provided ids will be loaded 
		'source': name of input stream (string - see set_stream() docstring)
		'mongodb_uri': URI of the server hosting mongod.
		   Example: 'mongodb://user:password@localhost:27017'
		'config': see ampel.pipeline.db.DBWired.load_config() docstring
		'central_db': see ampel.pipeline.db.DBWired.plug_central_db() docstring
		'publish_stats': publish performance stats:
		   * graphite: send t0 metrics to graphite (graphite server must be defined 
		     in Ampel_config)
		   * jobs: include t0 metrics in job document
		"""

		# Setup logger
		self.logger = LoggingUtils.get_logger(unique=True)
		self.ilb = InitLogBuffer()
		self.logger.addHandler(self.ilb)
		self.logger.info("Setting up new AlertProcessor instance")

		# Setup instance variable referencing ampel databases
		self.plug_databases(self.logger, mongodb_uri, config, central_db)

		# Load channels
		cl = ChannelLoader(self.config, source=source, tier=0)
		self.channels = cl.load_channels(channels, self.logger);
		self.chan_enum = list(enumerate(self.channels))
		self.chan_auto_complete = len(self.channels) * [False]
		self.live_ac = False

		for i, channel in self.chan_enum:
			if channel.has_source(source):
				ac = channel.get_config("parameters.autoComplete", source)
				if ac is not None and ac == "live":
					self.live_ac = True
					self.chan_auto_complete[i] = True

		# Robustness
		if len(self.channels) == 0:
			raise ValueError("No channel loaded, please check your config")
			return

		# Setup source dependant parameters
		self.set_source(source, load_ingester=load_ingester)

		# Which stats to publish (see doctring)
		self.publish_stats = publish_stats

		self.logger.info("AlertProcessor initial setup completed")


	def get_channels(self):
		return self.channels


	def set_source(self, source, load_ingester=True):
		"""
		Depending on which instrument and institution the alerts originate,
		(as of March 2018 only ZTF & IPAC), this method performs the following:
		-> defines the alert loading function.
		-> sets required static settings in AmpelAlert
		-> instantiates the adequate ingester class
		"""

		if source == "ZTFIPAC":

			# TODO: log something ? 
			conf = self.global_config['sources'][source]

			# Set static AmpelAlert alert flags
			alert_flags = None
			for flag_str in conf['alerts']['flags']:
				if alert_flags is None:
					alert_flags = AlertFlags[flag_str]
				else:
					alert_flags |= AlertFlags[flag_str]
			AmpelAlert.add_class_flags(alert_flags)

			# Set static AmpelAlert dict keywords
			AmpelAlert.set_alert_keywords(conf['alerts']['mappings'])
	
			if load_ingester:
				self.ingester = ZIAlertIngester(
					self.config, self.central_db, self.channels, logger=self.logger
				)
	
		else:
			# more sources may be defined later
			raise ValueError("Source '%s' not supported yet" % source)


	def set_ingester_instance(self, ingester_instance):
		"""
		Sets custom ingester instance to be used in the method run().
		If unspecified, a new instance of ZIAlertIngester() is used
		Known ingester (as for Sept 2017) are:
			* t0.ingesters.MemoryIngester
			* t0.ingesters.ZIAlertIngester
		"""
		self.ingester = ingester_instance


	def process_alert_folder(
		self, base_dir="/Users/hu/Documents/ZTF/Ampel/alerts/", 
		extension="*.avro", serialization="avro", source="ZTFIPAC", 
		max_entries=None, console_logging=True
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

		from ampel.pipeline.t0.alerts.DirAlertLoader import DirAlertLoader
		import importlib

		# Container class allowing to conveniently iterate over local avro files 
		alert_loader = DirAlertLoader(self.logger)
		alert_loader.set_folder(base_dir)
		alert_loader.set_extension(extension)

		if max_entries is not None:
			alert_loader.set_max_entries(max_entries)
		
		self.logger.info("Returning iterable for file paths in folder: %s" % base_dir)

		if source not in self.config['global']['sources']:
			raise ValueError("Unknown source %s, please check your config" % source)

		# Instantiate class shaping alert dicts
		if not hasattr(self, "alert_shaper"):
			shaper_class_full_path = self.config['global']['sources'][source]['alerts']['processing']['shape']
			shaper_module = importlib.import_module(shaper_class_full_path)
			shaper_class = getattr(shaper_module, shaper_class_full_path.split(".")[-1])
			self.alert_shaper = shaper_class(self.logger)

		als = AlertSupplier(alert_loader, self.alert_shaper, serialization=serialization)
		ret = AlertProcessor.iter_max
		count = 0

		while ret == AlertProcessor.iter_max:
			ret = self.run(als, console_logging)
			count += ret

		return count


	def run(self, alert_supplier, console_logging=True):
		"""
		For each alert:
			* Load the alert
			* Filter alert and set policies for every configured channels (defined by load_config())
			* Ingest alert based on PipelineIngester (default) 
			or the ingester instance set by the method set_ingester(obj)
		"""

		# Save current time to later evaluate how low was the pipeline processing time
		time_now = time.time
		run_start = time_now()


		# Part 1: Setup logging 
		#######################

		self.logger.info("Executing run method")

		if getattr(self, "ingester", None) is None:
			raise ValueError("No ingester instance was loaded")

		if not console_logging:
			self.logger.propagate = False

		# Remove logger saving "log headers" before job(s) 
		self.logger.removeHandler(self.ilb)

		# Create JobReporter instance
		db_job_reporter = DBJobReporter(self.central_db['logs'])

		# Create new job document in the logs collection
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
		scheduled_t2_units = len(self.channels) * [None]

		# Save ampel 'state' and get list of tran ids required for autocomplete
		if self.live_ac:
			tran_ids_before = self.get_tran_ids()

		# Forward jobId to ingester instance 
		# (will be inserted in the transient documents)
		self.ingester.set_job_id(
			db_job_reporter.get_job_id()
		)

		# Loop variables
		iter_max = AlertProcessor.iter_max
		iter_count = 0
		ingested_count = 0
		pps_loaded = 0
		uls_loaded = 0

		# metrics dict
		loop_stats = {}
		job_info = {
			'duration': {
				't0Job': {},
				'ingestion': {
					'preIngestTime': [],
					'dbBulkTimePhoto': [],
					'dbBulkTimeMain': [],
					'dbPerOpMeanTimePhoto': [],
					'dbPerOpMeanTimeMain': []
				},
				'filtering': {
					'all': np.empty(iter_max)
				}
			},
			'count': {
				# Cumul stats
				't0Job': {},	
				'ingestion': {
					'alerts': {},
					'dbOps': {}
				}
			}
		}

		job_info['duration']['filtering']['all'].fill(np.nan)
		for i, channel in self.chan_enum: 
			# job_info[channel.name] records filter performances
			# nan will remain only if exception occur for particular alerts
			job_info['duration']['filtering'][channel.name] = np.empty(iter_max)
			job_info['duration']['filtering'][channel.name].fill(np.nan)
			job_info['count']['ingestion']['alerts'][channel.name] = 0

		# The (standard) ingester will update DB insert operations
		if hasattr(self.ingester, "set_stats_dict"):
			self.ingester.set_stats_dict(
				job_info['duration']['ingestion'],
				job_info['count']['ingestion']['dbOps'],
			)

		# python shortcuts and micro-optimization
		loginfo = self.logger.info
		logdebug = self.logger.debug
		dblh_set_tranId = db_logging_handler.set_tranId
		dblh_set_channel = db_logging_handler.set_channels
		dblh_unset_tranId = db_logging_handler.unset_tranId
		dblh_unset_channel = db_logging_handler.unset_channels
		chan_enum = self.chan_enum
		filtering_stats = job_info['duration']['filtering']
		job_count_stats = job_info['count']['t0Job']
		alert_counts = job_info['count']['ingestion']['alerts']
		col = self.central_db['main']

		# Save pre run time
		pre_run = time_now()


		# Part 3: Process alerts
		########################

		self.logger.info("#######     Processing alerts     #######")

		# Iterate over alerts
		for shaped_alert in alert_supplier:

			# Associate upcoming log entries with the current transient id
			tran_id = shaped_alert['tran_id']
			dblh_set_tranId(tran_id)

			# Feedback
			loginfo(
				"Processing alert: %s (%s)" % 
				(shaped_alert['alert_id'], shaped_alert['ztf_id'])
			)

			# Create AmpelAlert instance
			ampel_alert = AmpelAlert(
				tran_id, shaped_alert['ro_pps'], shaped_alert['ro_uls']
			)

			# Update cumul stats
			pps_loaded += len(shaped_alert['pps'])
			if shaped_alert['uls'] is not None:
				uls_loaded += len(shaped_alert['uls'])

			# stats
			all_filters_start = time_now()

			# Loop through initialized channels
			for i, channel in chan_enum:

				# Associate upcoming log entries with the current channel
				dblh_set_channel(channel.name)

				try:

					# stats
					per_filter_start = time_now()

					# Apply filter (returns None in case of rejection or t2 runnable ids in case of match)
					scheduled_t2_units[i] = channel.filter_func(ampel_alert)

					# stats
					filtering_stats[channel.name][iter_count] = time_now() - per_filter_start

					# Log feedback and count
					if scheduled_t2_units[i] is not None:
						alert_counts[channel.name] += 1
						loginfo(channel.log_accepted)
					else:
						# Autocomplete required for this channel
						if self.live_ac and self.chan_auto_complete[i] and tran_id in tran_ids_before[i]:
							loginfo(channel.log_auto_complete)
							alert_counts[channel.name] += 1
							scheduled_t2_units[i] = channel.t2_units
						else:
							loginfo(channel.log_rejected)

				except:

					self.report_exception(
						{
							'section': 'filter',
							'channel': channel.name,
							'tranId': tran_id,
							'jobId':  db_job_reporter.get_job_id(),
						},
						shaped_alert = shaped_alert
					)

				# Unset channel id <-> log entries association
				dblh_unset_channel()

			# time required for all filters
			filtering_stats['all'][iter_count] = time_now() - all_filters_start

			if any(t2 is not None for t2 in scheduled_t2_units):

				# Ingest alert
				loginfo(" -> Ingesting alert")

				# stats
				ingested_count += 1

				# TODO: build tran_id <-> alert_id map (replayibility)
				#processed_alert[tran_id]
				try: 
					self.ingester.ingest(
						tran_id, shaped_alert['pps'], shaped_alert['uls'], scheduled_t2_units
					)
				except:
					self.report_exception(
						{
							'section': 'ingest',
							'tranId': tran_id,
							'jobId':  db_job_reporter.get_job_id(),
						},
						shaped_alert = shaped_alert
					)

			# Unset log entries association with transient id
			dblh_unset_tranId()

			iter_count += 1

			if iter_count == iter_max:
				self.logger.info("Reached max number of iterations")
				break

		# Save post run time
		post_run = time_now()

		# Post run section
		try: 

			# Optional autocomplete
			if self.live_ac:
				tran_ids_after = self.get_tran_ids()
	
				# Check post auto-complete
				for i, channel in chan_enum:
					if type(tran_ids_after[i]) is set:
						auto_complete_diff = tran_ids_after[i] - tran_ids_before[i]
						if auto_complete_diff:
							# TODO: implement fetch from archive
							pass 
	
			# Durations in seconds
			job_info['duration']['t0Job'] = {
				'preLoop': int(pre_run - run_start),
				'main': int(post_run - pre_run)
				# post loop will be computed in gather_and_send_stats()
			}

			if self.publish_stats is not None and iter_count > 0:
	
				self.logger.info("Computing job stats")
	
				# Gather counts
				job_info['count']['t0Job'] = {
					'alProcessed': iter_count,
					'alIngested': ingested_count,
					'ppsLoaded': pps_loaded,
					'ulsLoaded': uls_loaded
				}

				# Compute mean time & std dev in microseconds
				#############################################

				# For ingest metrics
				ingestion_stats = job_info['duration']['ingestion']
				for key in (
					'preIngestTime', 'dbBulkTimePhoto', 'dbBulkTimeMain', 
					'dbPerOpMeanTimePhoto', 'dbPerOpMeanTimeMain'
				):
					if len(ingestion_stats[key]) > 0: 
						ingestion_stats[key] = self.compute_stat(ingestion_stats[key])

				# For filter metrics
				len_non_nan = lambda x: iter_max - np.count_nonzero(np.isnan(x))
				for key in [channel.name for channel in self.channels] + ['all']:
					mylen = len_non_nan(filtering_stats[key])
					filtering_stats[key] = (
						(0, 0) if len_non_nan(filtering_stats[key]) == 0
						else self.compute_stat(
							filtering_stats[key], mean=np.nanmean, std=np.nanstd
						)
					)

				self.gather_and_send_stats(post_run, job_info)

			# Insert job info into job document
			db_job_reporter.set_job_stats("stats", job_info)

		except:

			self.report_exception(
				{
					'section': 'run_end',
					'jobId':  db_job_reporter.get_job_id(),
				}
			)
			

		# re-add initial log buffer
		self.logger.addHandler(self.ilb)

		loginfo(
			"Alert processing completed (time required: %is)" % 
			int(time_now() - run_start)
		)

		# Restore console logging if it was removed
		if not console_logging:
			self.logger.propagate = True

		# Remove DB logging handler
		if iter_count > 0:
			db_job_reporter.set_flush_job_info()
			db_logging_handler.flush()
		self.logger.removeHandler(db_logging_handler)
		
		# Return number of processed alerts
		return iter_count


	def gather_and_send_stats(self, post_run, t0_stats):
		"""
		"""

		t0_stats['duration']['t0Job']['postLoop'] = int(time.time() - post_run)

		# Publish metrics to graphite
		if "graphite" in self.publish_stats:

			self.logger.info("Sending stats to Graphite")

			# Re-using GraphiteClient results in: 
			# GraphiteSendException: Socket closed before able to send data to ('localhost', 52003), 
			# with error: [Errno 32] Broken pipe
			# So we re-create a GraphiteClient every time we send something to graphite...
			gfeeder = self.config['resources']['graphite']()

			if t0_stats is not None:
				gfeeder.add_stats_with_mean_std(t0_stats)
			else:
				gfeeder.add_stats(t0_stats)

			gfeeder.send()


	def report_exception(self, further_info=None, shaped_alert=None):
		"""
		further_info: non-nested dict instance
		"""
		import traceback

		self.logger.propagate = True
		self.logger.critical("Exception occured", exc_info=1)

		exception_str = traceback.format_exc().replace("\"", "'")

		insert_dict = {
			'tier': 0,
			'exception': exception_str.split("\n")
		}

		if shaped_alert is not None:

			if 'alert_id' in shaped_alert:
				insert_dict['alertId'] = shaped_alert['alert_id'] 
	
			if not "KeyboardInterrupt" in exception_str:
	
				if (
					'pps' in shaped_alert and 
					len(shaped_alert['pps']) > 0 and 
					'jd' in shaped_alert['pps'][0]
				):
					insert_dict['alertDt'] = shaped_alert['pps'][0]['jd']
		
				if further_info is not None:
					for key in further_info:
						insert_dict[key] = further_info[key]
		
				insert_dict['alertPPS'] = shaped_alert['pps']
				insert_dict['alertULS'] = shaped_alert['uls']

		self.get_trouble_col().insert_one(insert_dict)
		self.logger.propagate = False


	def compute_stat(self, seq, mean=np.mean, std=np.std):
		"""
		"""
		# mean time & std dev in microseconds
		np_seq = np.array(seq)
		return (
			int(round(mean(seq) * 1000000)),
			int(round(std(seq) * 1000000)) 
		)


	def get_tran_ids(self):
		"""
		Return value:
		Array whose length equals len(self.channels), possibly containing sets of transient ids.
		If channel[i] is the channel with index i wrt the list of channels 'self.channels', 
		and if channel[i] was configured to make use of the 'live' auto_complete feature, 
		then tran_ids[i] will hold a {set} of transient ids listing all known 
		transients currently available in the DB for this particular channel.
		Otherwise, tran_ids_before[i] will be None
		"""

		col = self.central_db['main']
		tran_ids = len(self.channels) * [None]

		# Loop through activated channels
		for i, channel in self.chan_enum:

			if self.chan_auto_complete[i]:

				# Build set of transient ids for this channel
				tran_ids[i] = {
					el['tranId'] for el in col.find(
						{
							'tranId': {'$gt': 1}, 
							'alDocType': AlDocTypes.TRANSIENT, 
							'channels': channel.name
						},
						{
							'_id': 0, 'tranId': 1
						}
					)
				}

		return tran_ids


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
	client = MongoClient(
		host, username=os.environ.get('MONGO_INITDB_ROOT_USERNAME', 'root'), 
		password=docker_env('MONGO_INITDB_ROOT_PASSWORD')
	)
	
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

def run_alertprocessor():

	import os, time, uuid
	from ampel.pipeline.config.ConfigLoader import AmpelArgumentParser
	
	parser = AmpelArgumentParser()
	action = parser.add_mutually_exclusive_group(required=True)
	action.add_argument('--broker', default='epyc.astro.washington.edu:9092')
	action.add_argument('--tarfile', default=None)
	parser.add_argument('--group', default=uuid.uuid1(), help="Kafka consumer group name")
	
	parser.require_resource('mongo', roles=['writer'])
	parser.require_resource('archive', roles=['writer'])
	parser.require_resource('graphite')
	# partially parse command line to get config
	opts, argv = parser.parse_known_args()
	# flesh out parser with resources required by t0 units
	loader = ChannelLoader(opts.config, source="ZTFIPAC", tier=0)
	parser.require_resources(*loader.get_required_resources())
	# parse again
	opts = parser.parse_args()

	mongo = opts.config['resources']['mongo']()['writer']
	archive = opts.config['resources']['archive']()['writer']
	
	from ampel.pipeline.t0.alerts.TarAlertLoader import TarAlertLoader
	from ampel.pipeline.t0.alerts.AlertSupplier import AlertSupplier
	from ampel.pipeline.t0.alerts.ZIAlertShaper import ZIAlertShaper
	from ampel.pipeline.t0.ZIAlertFetcher import ZIAlertFetcher
	from ampel.archive import ArchiveDB

	import time
	count = 0
	#AlertProcessor.iter_max = 100
	alert_processed = AlertProcessor.iter_max
	if opts.tarfile is not None:
		infile = opts.tarfile
		loader = TarAlertLoader(tar_path=opts.tarfile)
	else:
		infile = '{} group {}'.format(opts.broker, opts.group)
		fetcher = ZIAlertFetcher(opts.broker, group_name=opts.group, timeout=3600)
		loader = iter(fetcher)

	alert_supplier = AlertSupplier(loader, ZIAlertShaper(), serialization="avro", archive=ArchiveDB(archive))
	processor = AlertProcessor(mongodb_uri=mongo, publish_stats=["jobs", "graphite"], config=opts.config)

	while alert_processed == AlertProcessor.iter_max:
		t0 = time.time()
		print('Running on {}'.format(infile))
		try:
			alert_processed = processor.run(alert_supplier, console_logging=False)
		finally:
			t1 = time.time()
			dt = t1-t0
			print('({}) {} alerts in {:.1f}s; {:.1f}/s'.format(infile, alert_processed, dt, alert_processed/dt))
