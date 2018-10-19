#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/t0/AlertProcessor.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 10.10.2017
# Last Modified Date: 16.10.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import time, pkg_resources, numpy as np
from datetime import datetime
from ampel.pipeline.logging.AmpelLogger import AmpelLogger
from logging import LogRecord, INFO
from ampel.pipeline.logging.LoggingUtils import LoggingUtils
from ampel.pipeline.logging.RecordsBufferingHandler import RecordsBufferingHandler
from ampel.pipeline.logging.LogsBufferingHandler import LogsBufferingHandler
from ampel.pipeline.logging.DBLoggingHandler import DBLoggingHandler
from ampel.pipeline.logging.DBEventDoc import DBEventDoc
from ampel.pipeline.db.AmpelDB import AmpelDB
from ampel.pipeline.config.AmpelConfig import AmpelConfig
from ampel.pipeline.config.channel.ChannelConfigLoader import ChannelConfigLoader
from ampel.pipeline.t0.Channel import Channel
from ampel.pipeline.common.AmpelUtils import AmpelUtils
from ampel.pipeline.common.GraphiteFeeder import GraphiteFeeder
from ampel.core.abstract.AbsT0Setup import AbsT0Setup
from ampel.core.flags.LogRecordFlags import LogRecordFlags
from ampel.core.flags.AlDocType import AlDocType
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

	def __init__(self, 
		survey_id, channels=None, publish_stats=['graphite', 'jobs']
	):
		"""
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

		:param bool db_logging: whether to save log entries (and the corresponding job doc) into the DB
		"""

		# Setup logger
		self.logger = AmpelLogger.get_unique_logger()
		lbh = LogsBufferingHandler(tier=0)
		self.logger.addHandler(lbh)

		self.logger.info("Setting up new AlertProcessor instance")

		# Set input_setup
		if isinstance(survey_id, AbsT0Setup):
			self.input_setup = survey_id
			survey_id = self.input_setup.survey_id
		else:
			self.input_setup = next(
				pkg_resources.iter_entry_points(
					'ampel.pipeline.t0.sources', survey_id
				), None
			).resolve()()

		# Load channels
		self.t0_channels = [
			# Create Channel instance (instantiates channel's filter class as well)
			Channel(channel_config, survey_id, self.logger) 
			for channel_config in ChannelConfigLoader.load_configurations(channels, 0, self.logger)
		]

		AmpelDB.enable_rejected_collections(
			[chan.name for chan in self.t0_channels]	
		)

		# Shortcuts
		self.chan_enum = list(enumerate(self.t0_channels))
		self.chan_auto_complete = len(self.t0_channels) * [False]
		self.live_ac = False

		# Live auto-complete
		for i, channel in self.chan_enum:
			if channel.auto_complete == "live":
				self.live_ac = True
				self.chan_auto_complete[i] = True

		# Robustness
		if len(self.t0_channels) == 0:
			raise ValueError("No channel loaded, please check your config")

		# Which stats to publish (see doctring)
		self.publish_stats = publish_stats

		# Graphite
		if "graphite" in publish_stats:
			self.gfeeder = GraphiteFeeder(
				AmpelConfig.get_config('resources.graphite.default'),
				autoreconnect = True
			)

		self.logger.info("AlertProcessor setup completed")
		self.logger.removeHandler(lbh)
		self.log_headers = lbh.log_dicts


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

		# Create DB logging handler instance (logging.Handler child class)
		# This class formats, saves and pushes log records into the DB
		db_logging_handler = DBLoggingHandler(
			self.input_setup.get_log_flags() |
			LogRecordFlags.T0 | 
			LogRecordFlags.CORE
		)

		db_logging_handler.add_headers(self.log_headers)
		run_id = db_logging_handler.get_run_id()

		# Add db logging handler to the logger stack of handlers 
		self.logger.addHandler(db_logging_handler)

		self.logger.shout("Starting")

		if ingester is None:
			ingester = self.input_setup.get_alert_ingester(self.t0_channels, self.logger)

		if not full_console_logging:
			self.logger.quieten_console()

		# New document in the 'events' collection
		db_job_doc = DBEventDoc(event_name="ap", tier=0)
		db_job_doc.add_run_id(run_id)

		# Forward jobId to ingester instance 
		# (will be inserted in the transient documents)
		ingester.set_log_id(run_id)

		# Add current run_id to channel specific rejected log saver
		for t0_chan in self.t0_channels:
			t0_chan.rejected_logs_saver.set_run_id(run_id)


		# divers
		########

		# Create array
		scheduled_t2_units = len(self.t0_channels) * [None]

		# Save ampel 'state' and get list of tran ids required for autocomplete
		if self.live_ac:
			tran_ids_before = self.get_tran_ids()

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

		# Save pre run time
		pre_run = time_now()


		# Process alerts
		################

		self.logger.info("#######     Processing alerts     #######")

		# Iterate over alerts
		for alert_content in self.input_setup.get_alert_supplier(alert_loader):

			# Associate upcoming log entries with the current transient id
			tran_id = alert_content['tran_id']
			self.logger._AmpelLogger__tran_id = tran_id

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

				channel.set_log_extra(
					{
						'tranId': tran_id, 
						'alertId': alert_content['alert_id']
					}
				)

				try:

					# stats
					per_filter_start = time_now()

					# Apply filter (returns None in case of rejection or t2 runnable ids in case of match)
					scheduled_t2_units[i] = channel.filter_func(ampel_alert)

					# stats
					dur_stats['filters'][channel.name][iter_count] = time_now() - per_filter_start

					# Filter rejected alert
					if scheduled_t2_units[i] is None:

						# Autocomplete required for this channel
						if self.live_ac and self.chan_auto_complete[i] and tran_id in tran_ids_before[i]:
							channel.buff_logger.info("Live ac")
							count_stats['matches'][channel.name] += 1
							scheduled_t2_units[i] = channel.t2_units

						else:

							# Save possibly existing error to 'main' logs
							if channel.buff_handler.has_error:
								channel.buff_handler.copy(db_logging_handler, True)

							# If channel did not log anything, do it for it
							if not channel.buff_handler.buffer:
								channel.buff_logger.info(None)

							# Save rejected logs to separate (channel specific) db collection
							channel.buff_handler.forward(
								channel.rejected_logs_saver
							)

					# Filter accepted alert
					else:

						count_stats['matches'][channel.name] += 1

						# If channel did not log anything, do it for it
						if not channel.buff_handler.buffer:
							channel.buff_logger.info(None)

						# Write log entries from buffer handler to main log collection
						channel.buff_handler.forward(db_logging_handler, True)

				except Exception as e:

					channel.buff_handler.forward(db_logging_handler)
					LoggingUtils.report_exception(
						self.logger, e, tier=0, 
						run_id=db_logging_handler.get_run_id(), info={
							'section': 'ap_filter',
							'channel': channel.name,
							'tranId': tran_id,
							'alert': AlertProcessor._alert_essential(alert_content)
						}
					)

			# time required for all filters
			dur_stats['allFilters'][iter_count] = time_now() - all_filters_start

			if any(t2 is not None for t2 in scheduled_t2_units):

				# stats
				ingested_count += 1

				# TODO: build tran_id <-> alert_id map (replayability)
				#processed_alert[tran_id]
				try: 
					ingester.ingest(
						tran_id, alert_content['pps'], 
						alert_content['uls'], 
						scheduled_t2_units
					)
				except Exception as e:
					LoggingUtils.report_exception(
						self.logger, e, tier=0, 
						run_id=db_logging_handler.get_run_id(), info={
							'section': 'ap_ingest',
							'tranId': tran_id,
							'alert': AlertProcessor._alert_essential(alert_content)
						}
					)
			else:

				# If all channels reject this alert, no log entries goes into
				# the main logs collection sinces those are redirected to Ampel_rej.
				# So we add a notification manually. For that, we don't use self.logger 
				# cause rejection messages were alreary logged into the console 
				# by the StreamHandler in channel specific RecordsBufferingHandler instances. 
				# So we address directly db_logging_handler, and for that, we create
				# a LogRecord manually.
				lr = LogRecord(None, INFO, None, None, "Done", None, None)
				lr.extra = {
					'tranId': tran_id,
					'alertId': alert_content['alert_id']
				}
				db_logging_handler.handle(lr)

			# Unset log entries association with transient id
			self.logger._AmpelLogger__tran_id = None

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
					db_job_doc.set_event_info(
						{
							'metrics': {
								"count": count_stats,
								"duration": dur_stats
							}
						}
					)
		except Exception as e:
			LoggingUtils.report_exception(
				self.logger, e, tier=0, run_id=db_logging_handler.get_run_id()
			)
			
		self.logger.shout(
			"Alert processing completed (time required: %is)" % 
			int(time_now() - run_start)
		)

		# Restore console logging settings
		if not full_console_logging:
			self.logger.louden_console()

		# Flush loggers
		if iter_count > 0:

			# Main logs logger
			db_logging_handler.flush()

			# Flush channel-specific rejected logs
			for t0_chan in self.t0_channels:
				t0_chan.rejected_logs_saver.flush()

		else:
			db_logging_handler.purge()

		# Remove DB logging handler
		self.logger.removeHandler(db_logging_handler)

		db_job_doc.publish()

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
		:returns: array whose length equals len(self.t0_channels), possibly containing sets of transient ids.
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
							'alDocType': AlDocType.TRANSIENT, 
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
