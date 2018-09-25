#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/t0/AlertProcessor.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 10.10.2017
# Last Modified Date: 24.09.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import time, pkg_resources, numpy as np
from datetime import datetime
from ampel.pipeline.logging.LoggingUtils import LoggingUtils
from ampel.pipeline.logging.DBLoggingHandler import DBLoggingHandler
from ampel.pipeline.db.AmpelDB import AmpelDB
from ampel.pipeline.config.AmpelConfig import AmpelConfig
from ampel.pipeline.config.channel.ChannelLoader import ChannelLoader
from ampel.pipeline.config.channel.T0Channel import T0Channel
from ampel.pipeline.common.AmpelUtils import AmpelUtils
from ampel.pipeline.common.GraphiteFeeder import GraphiteFeeder
from ampel.core.abstract.AbsT0Setup import AbsT0Setup
from ampel.core.flags.AlDocTypes import AlDocTypes
from ampel.base.AmpelAlert import AmpelAlert

class AlertProcessor():
	""" 
	Class handling T0 pipeline operations.

	For each alert, following tasks are performed:
		* Load the alert
		* Filter alert based on the configured T0 filter
		* Ingest alert based on the configured ingester
	"""
	iter_max = 5000

	def __init__(self, survey_id, channels=None, central_db=None,
		publish_stats=['graphite', 'jobs'], db_logging=True
	):
		"""
		Parameters:
		-----------
		:param str survey_id: id of the survey (ex: 'ZTFIPAC').
		Associated ressources will be loaded using the entry_point with id 'survey_id'
		defined by ampel plugins such as Ampel-ZTF (ampel.pipeline.t0.sources)
		:param channels: 
		- None: all the available channels from the ampel config will be loaded
		- String: channel with the provided id will be loaded
		- List of strings: channels with the provided ids will be loaded 
		:param publish_stats: publish performance stats:
		- graphite: send t0 metrics to graphite (graphite server must be defined 
		  in Ampel_config)
		- jobs: include t0 metrics in job document
		:param central_db: string. Use provided DB name rather than Ampel default database ('Ampel')
		:param db_logging: bool.
		"""

		# Setup logger
		self.logger = LoggingUtils.get_logger(unique=True)

		if db_logging:

			# Create DB logging handler instance (logging.Handler child class)
			# This class formats, saves and pushes log records into the DB
			self.db_logging_handler = DBLoggingHandler(tier=0)

			# Add db logging handler to the logger stack of handlers 
			self.logger.addHandler(self.db_logging_handler)

		self.logger.info("Setting up new AlertProcessor instance")

		# Set input_setup
		self.input_setup = next(
			pkg_resources.iter_entry_points(
				'ampel.pipeline.t0.sources', survey_id
			), None
		).resolve()()

		# Load channels
		self.t0_channels = [
			T0Channel(channel_config, survey_id, self.logger) 
			for channel_config in ChannelLoader.load_channels(channels, self.logger)
		]
		self.chan_enum = list(enumerate(self.t0_channels))
		self.chan_auto_complete = len(self.t0_channels) * [False]
		self.live_ac = False

		for i, channel in self.chan_enum:
			if channel.auto_complete == "live":
				self.live_ac = True
				self.chan_auto_complete[i] = True

		# Robustness
		if len(self.t0_channels) == 0:
			raise ValueError("No channel loaded, please check your config")

		# Optional override of AmpelConfig defaults
		if central_db is not None:
			AmpelDB.set_central_db_name(central_db)

		# Which stats to publish (see doctring)
		self.publish_stats = publish_stats

		# Graphite
		if "graphite" in publish_stats:
			self.gfeeder = GraphiteFeeder(
				AmpelConfig.get_config('resources.graphite.default'),
				autoreconnect = True
			)

		self.logger.info("AlertProcessor initial setup completed")


	def run(self, alert_loader, ingester=None, full_console_logging=True):
		"""
		Run the alert processing using the provided alert_loader
		:param alert_loader: iterable instance that returns alert payloads
		:param ingester: sets a custom ingester instance. 
		If unspecified, the ingester loaded is the default ingester associated with 
		the current AbsInputStreamSetup. For ZTFIPAC: a new instance of ZIAlertIngester() is used:
		Other possible ingester (as of Sept 2018) ampel.pipeline.t0.ingest.MemoryIngester
		:param full_console_logging: bool. If false, the logging level of the stdout streamhandler 
		associated with the logger will be set to WARN during the execution of this method
		(it will be reverted to DEBUG before return)
		"""

		# Save current time to later evaluate how low was the pipeline processing time
		time_now = time.time
		run_start = time_now()


		# Part 1: Setup logging 
		#######################

		self.logger.info("Executing run method")

		if ingester is None:
			ingester = self.input_setup.get_alert_ingester(self.t0_channels, self.logger)

		if not full_console_logging:
			LoggingUtils.quieten_console_logger(self.logger)


		# Part 2: divers
		################

		# Create array
		scheduled_t2_units = len(self.t0_channels) * [None]

		# Save ampel 'state' and get list of tran ids required for autocomplete
		if self.live_ac:
			tran_ids_before = self.get_tran_ids()

		# Forward jobId to ingester instance 
		# (will be inserted in the transient documents)
		ingester.set_log_id(
			self.db_logging_handler.get_run_id()
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
		if hasattr(ingester, "set_stats_dict"):
			ingester.set_stats_dict(
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
		for alert_content in self.input_setup.get_alert_supplier(alert_loader):

			# Associate upcoming log entries with the current transient id
			tran_id = alert_content['tran_id']
			self.db_logging_handler.set_tran_id(tran_id)

			# Feedback
			log_info(
				"Processing alert: %s" % alert_content['alert_id']
			)

			# Create AmpelAlert instance
			ampel_alert = AmpelAlert(
				tran_id, alert_content['ro_pps'], alert_content['ro_uls']
			)

			# Update cumul stats
			pps_loaded += len(alert_content['pps'])
			if alert_content['uls'] is not None:
				uls_loaded += len(alert_content['uls'])

			# stats
			all_filters_start = time_now()

			# Loop through initialized channels
			for i, channel in self.chan_enum:

				# Associate upcoming log entries with the current channel
				self.db_logging_handler.set_channels(channel.name)

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
							'logs':  self.db_logging_handler.get_run_id(),
							'alert': AlertProcessor._alert_essential(alert_content)
						}
					)

				# Unset channel id <-> log entries association
				self.db_logging_handler.unset_channels()

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
					ingester.ingest(
						tran_id, alert_content['pps'], alert_content['uls'], scheduled_t2_units
					)
				except:
					AmpelUtils.report_exception(
						self.logger, tier=0, info={
							'section': 'ap_ingest',
							'tranId': tran_id,
							'logs':  self.db_logging_handler.get_run_id(),
							'alert': AlertProcessor._alert_essential(alert_content)
						}
					)

			# Unset log entries association with transient id
			self.db_logging_handler.unset_tran_id()

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
				for key in [channel.name for channel in self.t0_channels]:
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
									'logs': self.db_logging_handler.get_run_id(),
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
					'logs':  self.db_logging_handler.get_run_id(),
				}
			)
			
		log_info(
			"Alert processing completed (time required: %is)" % 
			int(time_now() - run_start)
		)

		# Restore console logging settings
		if not full_console_logging:
			LoggingUtils.louden_console_logger(self.logger)

		# Remove DB logging handler
		if iter_count > 0:

			try:
				self.db_logging_handler.flush()
			except Exception as e:

				# error msg will be passed to the logging handlers of higher level
				# as self.logger.propagate was set back to True previously
				import logging
				logging.error("DB log flushing has failed")

				try: 
					# This will fail as well if we have DB connectivity issues
					AmpelUtils.report_exception(
						self.logger, tier=0, info={
							'section': 'ap_flush_logs',
							'logs':  self.db_logging_handler.get_run_id(),
						}
					)
				except Exception as e:
					self.logger.error("Could not add new doc to 'troubles' collection, DB offline?")
					self.logger.error(e)
		else:

			self.db_logging_handler.remove_log_entries()


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
		Array whose length equals len(self.t0_channels), possibly containing sets of transient ids.
		If channel[i] is the channel with index i wrt the list of channels 'self.t0_channels', 
		and if channel[i] was configured to make use of the 'live' auto_complete feature, 
		then tran_ids[i] will hold a {set} of transient ids listing all known 
		transients currently available in the DB for this particular channel.
		Otherwise, tran_ids_before[i] will be None
		"""

		col = AmpelDB.get_collection('main')
		tran_ids = len(self.t0_channels) * [None]

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
	def _alert_essential(alert_content):
		return {
			'id': alert_content.get('alert_id'),
			'pps': alert_content.get('pps'),
			'uls': alert_content.get('uls')
		}
