#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/t0/AlertProcessor.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 10.10.2017
# Last Modified Date: 13.07.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import pymongo, time, numpy as np
from datetime import datetime

from ampel.pipeline.t0.alerts.AlertSupplier import AlertSupplier
from ampel.pipeline.t0.alerts.ZIAlertShaper import ZIAlertShaper
from ampel.pipeline.t0.ingesters.ZIAlertIngester import ZIAlertIngester
from ampel.pipeline.logging.LoggingUtils import LoggingUtils
from ampel.pipeline.logging.DBLoggingHandler import DBLoggingHandler
from ampel.pipeline.logging.InitLogBuffer import InitLogBuffer
from ampel.pipeline.db.AmpelDB import AmpelDB
from ampel.pipeline.config.AmpelConfig import AmpelConfig
from ampel.pipeline.config.ChannelLoader import ChannelLoader
from ampel.pipeline.common.AmpelUtils import AmpelUtils
from ampel.pipeline.common.GraphiteFeeder import GraphiteFeeder
from ampel.base.AmpelAlert import AmpelAlert
from ampel.core.flags.AlDocTypes import AlDocTypes
from ampel.core.flags.FlagUtils import FlagUtils
from ampel.base.flags.AmpelFlags import AmpelFlags


class AlertProcessor():
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
		self, channels=None, source="ZTFIPAC", central_db=None,
		publish_stats=['graphite', 'jobs'], load_ingester=True
	):
		"""
		Parameters:
		-----------
		'channels': 
		   - None: all the available channels from the ampel config will be loaded
		   - String: channel with the provided id will be loaded
		   - List of strings: channels with the provided ids will be loaded 
		'source': name of input stream (string - see set_stream() docstring)
		'publish_stats': publish performance stats:
		   - graphite: send t0 metrics to graphite (graphite server must be defined 
		     in Ampel_config)
		   - jobs: include t0 metrics in job document
		'central_db': string. Use provided DB name rather than Ampel default database ('Ampel')
		"""

		# Setup logger
		self.logger = LoggingUtils.get_logger(unique=True)
		self.ilb = InitLogBuffer()
		self.logger.addHandler(self.ilb)
		self.logger.info("Setting up new AlertProcessor instance")

		# Load channels
		cl = ChannelLoader(source=source, tier=0)
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

		# Optional override of AmpelConfig defaults
		if central_db is not None:
			AmpelDB.set_central_db_name(central_db)

		# Setup source dependant parameters
		self.set_source(source, load_ingester=load_ingester)

		# Which stats to publish (see doctring)
		self.publish_stats = publish_stats

		# Graphite
		if "graphite" in publish_stats:
			self.gfeeder = GraphiteFeeder(
				AmpelConfig.get_config('resources.graphite.default'),
				autoreconnect = True
			)

		self.logger.info("AlertProcessor initial setup completed")


	def get_channels(self):
		""" """
		return self.channels


	def set_source(self, source, load_ingester=True):
		"""
		Depending on which instrument and institution the alerts originate,
		(as of March 2018 only ZTF & IPAC), this method performs the following:
		-> sets required static settings in AmpelAlert
		-> instantiates the adequate ingester class
		"""

		if source == "ZTFIPAC":

			# Set static AmpelAlert alert flags
			AmpelAlert.add_class_flags(
				FlagUtils.list_flags_to_enum_flags(
					AmpelConfig.get_config('global.sources.ZTFIPAC.alerts.flags'),
					AmpelFlags
				)
			)

			# Set static AmpelAlert dict keywords
			AmpelAlert.set_alert_keywords(
				AmpelConfig.get_config('global.sources.ZTFIPAC.alerts.mappings')
			)
	
			if load_ingester:
				self.ingester = ZIAlertIngester(self.channels, logger=self.logger)
	
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

		# Create DB logging handler instance (logging.Handler child class)
		# This class formats, saves and pushes log records into the DB
		db_logging_handler = DBLoggingHandler(
			tier=0, info={
				"alertProc": self.version, 
				"ingesterClass": str(self.ingester.__class__)
			}, previous_logs=self.ilb.get_logs()
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
		self.ingester.set_log_id(
			db_logging_handler.get_log_id()
		)

		# Loop variables
		iter_max = AlertProcessor.iter_max
		iter_count = 0
		ingested_count = 0
		pps_loaded = 0
		uls_loaded = 0

		# Duration statistics 
		dur_stats = {
			'preIngestTime': [],
			'dbBulkTimePhoto': [],
			'dbBulkTimeMain': [],
			'dbPerOpMeanTimePhoto': [],
			'dbPerOpMeanTimeMain': [],
			'allFilters': np.empty(iter_max),
			'filters': {}
		}

		# Count statistics (incrementing integer values)
		count_stats = {    
			'alerts': 0,
			'ingested': 0,
			'pps': 0,
			'uls': 0,
			'reprocs': 0,
			'matches': {},
			'dbUpd': {}
		}

		dur_stats['allFilters'].fill(np.nan)
		for i, channel in self.chan_enum: 
			# dur_stats['filters'][channel.name] records filter performances
			# nan will remain only if exception occur for particular alerts
			dur_stats['filters'][channel.name] = np.empty(iter_max)
			dur_stats['filters'][channel.name].fill(np.nan)
			count_stats['matches'][channel.name] = 0

		# The (standard) ingester will update DB insert operations
		if hasattr(self.ingester, "set_stats_dict"):
			self.ingester.set_stats_dict(
				dur_stats, count_stats['dbUpd']
			)

		# shortcuts
		log_info = self.logger.info
		log_debug = self.logger.debug

		# Save pre run time
		pre_run = time_now()


		# Part 3: Process alerts
		########################

		self.logger.info("#######     Processing alerts     #######")

		# Iterate over alerts
		for shaped_alert in alert_supplier:

			# Associate upcoming log entries with the current transient id
			tran_id = shaped_alert['tran_id']
			db_logging_handler.set_tranId(tran_id)

			# Feedback
			log_info(
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
			for i, channel in self.chan_enum:

				# Associate upcoming log entries with the current channel
				db_logging_handler.set_channels(channel.name)

				try:

					# stats
					per_filter_start = time_now()

					# Apply filter (returns None in case of rejection or t2 runnable ids in case of match)
					scheduled_t2_units[i] = channel.filter_func(ampel_alert)

					# stats
					dur_stats['filters'][channel.name][iter_count] = time_now() - per_filter_start

					# Log feedback and count
					if scheduled_t2_units[i] is not None:
						count_stats['matches'][channel.name] += 1
						log_info(channel.log_accepted)
					else:
						# Autocomplete required for this channel
						if self.live_ac and self.chan_auto_complete[i] and tran_id in tran_ids_before[i]:
							log_info(channel.log_auto_complete)
							count_stats['matches'][channel.name] += 1
							scheduled_t2_units[i] = channel.t2_units
						else:
							log_info(channel.log_rejected)

				except:

					AmpelUtils.report_exception(
						self.logger, tier=0, info={
							'section': 'ap_filter',
							'channel': channel.name,
							'tranId': tran_id,
							'logs':  db_logging_handler.get_log_id(),
							'alert': AlertProcessor._alert_essential(shaped_alert)
						}
					)

				# Unset channel id <-> log entries association
				db_logging_handler.unset_channels()

			# time required for all filters
			dur_stats['allFilters'][iter_count] = time_now() - all_filters_start

			if any(t2 is not None for t2 in scheduled_t2_units):

				# Ingest alert
				log_info(" -> Ingesting alert")

				# stats
				ingested_count += 1

				# TODO: build tran_id <-> alert_id map (replayability)
				#processed_alert[tran_id]
				try: 
					self.ingester.ingest(
						tran_id, shaped_alert['pps'], shaped_alert['uls'], scheduled_t2_units
					)
				except:
					AmpelUtils.report_exception(
						self.logger, tier=0, info={
							'section': 'ap_ingest',
							'tranId': tran_id,
							'logs':  db_logging_handler.get_log_id(),
							'alert': AlertProcessor._alert_essential(shaped_alert)
						}
					)

			# Unset log entries association with transient id
			db_logging_handler.unset_tranId()

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
				for i, channel in self.chan_enum:
					if type(tran_ids_after[i]) is set:
						auto_complete_diff = tran_ids_after[i] - tran_ids_before[i]
						if auto_complete_diff:
							# TODO: implement fetch from archive
							pass 
	
			if self.publish_stats is not None and iter_count > 0:
	
				# include loop counts
				count_stats['alerts'] = iter_count
				count_stats['ingested'] = ingested_count
				count_stats['pps'] = pps_loaded
				count_stats['uls'] = uls_loaded

				# Cleaner structure
				count_stats['ppReprocs'] = count_stats['dbUpd'].pop('ppReprocs')

				# Compute mean time & std dev in microseconds
				#############################################

				self.logger.info("Computing job stats")

				# For ingest metrics
				for key in ('preIngestTime', 'dbBulkTimePhoto', 'dbBulkTimeMain', 
					'dbPerOpMeanTimePhoto', 'dbPerOpMeanTimeMain'
				):
					if dur_stats[key]: 
						dur_stats[key] = self.compute_stat(dur_stats[key])

				# per chan filter metrics
				len_non_nan = lambda x: iter_max - np.count_nonzero(np.isnan(x))
				for key in [channel.name for channel in self.channels]:
					dur_stats['filters'][key] = self.compute_stat(
						dur_stats['filters'][key], mean=np.nanmean, std=np.nanstd
					)

				# all filters metric
				dur_stats['allFilters'] = self.compute_stat(
					dur_stats['allFilters'], mean=np.nanmean, std=np.nanstd
				)

				# Durations in seconds
				dur_stats['apPreLoop'] = int(pre_run - run_start)
				dur_stats['apMain'] = int(post_run - pre_run)
				dur_stats['apPostLoop'] = int(time.time() - post_run)

				# Publish metrics to graphite
				if "graphite" in self.publish_stats:
					self.logger.info("Sending stats to Graphite")
					self.gfeeder.add_stats_with_mean_std(
						{
							"count": count_stats,
							"duration": dur_stats
						},
						prefix="t0"
					)
					self.gfeeder.send()

				# Publish metrics into document in collection 'runs'
				if "jobs" in self.publish_stats:
					AmpelDB.get_collection('runs').update_one(
						{'_id': int(datetime.today().strftime('%Y%m%d'))},
						{
							'$push': {
								'jobs': {
									'tier': 0,
									'dt': datetime.utcnow().timestamp(),
									'logs': db_logging_handler.get_log_id(),
									'metrics': {
										"count": count_stats,
										"duration": dur_stats
									}
								}
							}
						},
						upsert=True
					)
		except:

			AmpelUtils.report_exception(
				self.logger, tier=0, info={
					'section': 'ap_run_end',
					'logs':  db_logging_handler.get_log_id(),
				}
			)
			

		# re-add initial log buffer
		self.logger.addHandler(self.ilb)

		log_info(
			"Alert processing completed (time required: %is)" % 
			int(time_now() - run_start)
		)

		# Restore console logging if it was removed
		if not console_logging:
			self.logger.propagate = True

		# Remove DB logging handler
		if iter_count > 0:

			try:
				db_logging_handler.flush()
			except Exception as e:

				# error msg will be passed to the logging handlers of higher level
				# as self.logger.propagate was set back to True previously
				self.logger.removeHandler(db_logging_handler)
				self.logger.error("DB log flushing has failed")
				self.logger.error(e)

				try: 
					# This will fail as well if we have DB connectivity issues
					AmpelUtils.report_exception(
						self.logger, tier=0, info={
							'section': 'ap_flush_logs',
							'logs':  db_logging_handler.get_log_id(),
						}
					)
				except Exception as e:
					self.logger.error("Could not add new doc to 'troubles' collection, DB offline?")
					self.logger.error(e)
		else:

			db_logging_handler.remove_log_entry()


		self.logger.removeHandler(db_logging_handler)
		
		# Return number of processed alerts
		return iter_count


	def compute_stat(self, seq, mean=np.mean, std=np.std):
		"""
		"""

		if AlertProcessor.iter_max - np.count_nonzero(np.isnan(seq)) == 0:
			return (0, 0)

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

		col = AmpelDB.get_collection('main')
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


	@staticmethod	
	def _alert_essential(shaped_alert):
		return {
			'id': shaped_alert.get('alert_id'),
			'pps': shaped_alert.get('pps'),
			'uls': shaped_alert.get('uls')
		}

def run_alertprocessor():

	import os, time, uuid
	from ampel.pipeline.config.ConfigLoader import AmpelArgumentParser
	from ampel.pipeline.config.AmpelConfig import AmpelConfig
	from ampel.archive import ArchiveDB

	parser = AmpelArgumentParser()
	parser.require_resource('mongo', ['writer', 'logger'])
	parser.require_resource('archive', ['writer'])
	parser.require_resource('graphite')
	action = parser.add_mutually_exclusive_group(required=True)
	action.add_argument('--broker', default='epyc.astro.washington.edu:9092')
	action.add_argument('--tarfile', default=None)
	parser.add_argument('--group', default=uuid.uuid1(), help="Kafka consumer group name")
	parser.add_argument('--channels', default=None, nargs="+")
	
	# partially parse command line to get config
	opts, argv = parser.parse_known_args()
	# flesh out parser with resources required by t0 units
	loader = ChannelLoader(source="ZTFIPAC", tier=0)
	parser.require_resources(*loader.get_required_resources())
	# parse again
	opts = parser.parse_args()

	from ampel.pipeline.t0.alerts.TarAlertLoader import TarAlertLoader
	from ampel.pipeline.t0.alerts.AlertSupplier import AlertSupplier
	from ampel.pipeline.t0.alerts.ZIAlertShaper import ZIAlertShaper
	from ampel.pipeline.t0.ZIAlertFetcher import ZIAlertFetcher

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

	alert_supplier = AlertSupplier(
		loader, ZIAlertShaper(), serialization="avro", 
		archive=ArchiveDB(AmpelConfig.get_config('resources.archive.writer'))
	)
	processor = AlertProcessor(publish_stats=["jobs", "graphite"], channels=opts.channels)

	while alert_processed == AlertProcessor.iter_max:
		t0 = time.time()
		print('Running on {}'.format(infile))
		try:
			alert_processed = processor.run(alert_supplier, console_logging=False)
		finally:
			t1 = time.time()
			dt = t1-t0
			print('({}) {} alerts in {:.1f}s; {:.1f}/s'.format(infile, alert_processed, dt, alert_processed/dt))


