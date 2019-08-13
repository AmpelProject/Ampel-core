#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/t0/AlertProcessor.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 10.10.2017
# Last Modified Date: 09.03.2019
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import pkg_resources, numpy as np
from time import time
from ampel.pipeline.logging.AmpelLogger import AmpelLogger
from logging import LogRecord, INFO
from pymongo.errors import PyMongoError
from ampel.pipeline.logging.LoggingUtils import LoggingUtils
from ampel.pipeline.logging.T0ConsoleFormatter import T0ConsoleFormatter
from ampel.pipeline.logging.RecordsBufferingHandler import RecordsBufferingHandler
from ampel.pipeline.logging.LogsBufferingHandler import LogsBufferingHandler
from ampel.pipeline.logging.DBLoggingHandler import DBLoggingHandler
from ampel.pipeline.logging.DBEventDoc import DBEventDoc
from ampel.pipeline.logging.AmpelLoggingError import AmpelLoggingError
from ampel.pipeline.db.AmpelDB import AmpelDB
from ampel.pipeline.db.DBUpdateError import DBUpdateError
from ampel.pipeline.config.AmpelConfig import AmpelConfig
from ampel.pipeline.config.channel.ChannelConfigLoader import ChannelConfigLoader
from ampel.pipeline.t0.ingest.DBUpdatesBuffer import DBUpdatesBuffer
from ampel.pipeline.t0.Channel import Channel
from ampel.pipeline.common.AmpelUtils import AmpelUtils
from ampel.pipeline.common.GraphiteFeeder import GraphiteFeeder
from ampel.core.abstract.AbsSurveySetup import AbsSurveySetup
from ampel.core.flags.LogRecordFlag import LogRecordFlag
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
	iter_max = 50000

	def __init__(self, 
		survey_id, channels=None, publish_stats=['graphite', 'jobs'], skip_t2_units={},
		log_line_nbr=False, single_rej_col=False, log_format="compact" 
	):
		"""
		:param str survey_id: id of the survey (ex: 'ZTFIPAC').
		Associated ressources will be loaded using the entry_point with id 'survey_id'
		defined by ampel plugins such as Ampel-ZTF (ampel.pipeline.sources)

		:param channels:

		- None: all the available channels from the ampel config will be loaded
		- String: channel with the provided id will be loaded
		- List of strings: channels with the provided ids will be loaded 

		:param List[str] publish_stats: publish performance stats:

		- graphite: send t0 metrics to graphite (graphite server must be defined 
		  in Ampel_config)
		- jobs: include t0 metrics in job document

		:param set skip_t2_units: do not create tickets for these T2 units
		:param bool db_logging: whether to save log entries (and the corresponding job doc) into the DB
		:param bool single_rej_col: 
			- False: rejected logs are saved in channel specific collections
			 (collection name equals channel name)
			- True: rejected logs are saved in a single collection called 'logs'
		:param str log_format: 'compact' (saves RAM by reducing the number of indexed document) or standard. \
		The 'compact' log entries can be later converted into 'standard' format using the aggregation pipeline.
		Avoid using 'compact' if you run the alert processor with a single channel.
		Examples:
			- 'compact': embed channel information withing log record 'msg' field. \
			```
			{
			    "_id" : ObjectId("5be4aa6254048041edbac352"),
    			"tranId" : NumberLong(1810101032122523),
    			"alertId" : NumberLong(404105201415015004),
    			"flag" : 572784643,
    			"runId" : 509,
    			"msg" : [ 
    			    {
    					"channels" : "NO_FILTER",
						"txt": "Alert accepted"
    			    },
    			    {
    					"channels" : "HU_RANDOM",
						"txt": "Alert accepted"
    				}
    			]
			}
			```
			- 'standard': channel info are encoded in log parameter 'extra'. \
			For a given alert, one log entry is created per channel since log concatenation \
			cannot happen (the 'extra' dicts from the two log entries differ): \
			```
			{
			    "_id" : ObjectId("5be4aa6254048041edbac353"),
    			"tranId" : NumberLong(1810101032122523),
    			"alertId" : NumberLong(404105201415015004),
    			"flag" : 572784643,
    			"runId" : 509,
    			"channels" : "NO_FILTER",
    			"msg" : "Alert accepted"
			}
			{
			    "_id" : ObjectId("5be4aa6254048041edbac352"),
    			"tranId" : NumberLong(1810101032122523),
    			"alertId" : NumberLong(404105201415015004),
    			"flag" : 572784643,
    			"runId" : 509,
    			"channels" : "HU_RANDOM",
    			"msg" : "Alert accepted"
			}
			```
		"""

		# Setup logger
		self.logger = AmpelLogger.get_unique_logger(
			formatter=T0ConsoleFormatter(line_number=log_line_nbr)
		)

		lbh = LogsBufferingHandler(tier=0)
		self.logger.addHandler(lbh)

		self.logger.info("Setting up new AlertProcessor instance")

		# Set input_setup
		if isinstance(survey_id, AbsSurveySetup):
			self.input_setup = survey_id
			survey_id = self.input_setup.survey_id
		else:
			self.input_setup = next(
				pkg_resources.iter_entry_points(
					'ampel.pipeline.sources', survey_id
				), None
			).resolve()()

		self.embed = log_format == "compact"

		# Load channels
		self.t0_channels = [
			# Create Channel instance (instantiates channel's filter class as well)
			Channel(
				channel_config, survey_id, self.logger, 
				log_line_nbr, self.embed, single_rej_col=single_rej_col
			)
			for channel_config in ChannelConfigLoader.load_configurations(channels, 0, self.logger)
		]

		if len(self.t0_channels) == 1 and log_format == "compact":
			self.logger.warning("You should not use log_format='compact' using only one channel")

		if single_rej_col:
			AmpelDB.enable_rejected_collections(['rejected'])
		else:
			AmpelDB.enable_rejected_collections(
				[chan.str_name for chan in self.t0_channels]
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


	def run(
		self, alert_loader, ingester=None, full_console_logging=True, 
		run_type=LogRecordFlag.SCHEDULED_RUN
	):
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

		:param LogRecordFlag run_type: LogRecordFlag.SCHEDULED_RUN or LogRecordFlag.MANUAL_RUN

		:rtype: int
		:raises: LogFlushingError, DBUpdateError, PyMongoError
		"""

		# Save current time to later evaluate how low was the pipeline processing time
		run_start = time()

		# Create DB logging handler instance (logging.Handler child class)
		# This class formats, saves and pushes log records into the DB
		db_logging_handler = DBLoggingHandler(
			LogRecordFlag.T0 | LogRecordFlag.CORE | run_type
		)

		db_logging_handler.add_headers(self.log_headers)
		run_id = db_logging_handler.get_run_id()

		# Add db logging handler to the logger stack of handlers 
		self.logger.handlers.insert(0, db_logging_handler)
		if not full_console_logging:
			self.logger.quieten_console_loggers()

		self.logger.shout("Starting")

		if ingester is None:
			ingester = self.input_setup.get_alert_ingester(
				self.t0_channels, self.logger
			)

		# New document in the 'events' collection
		db_job_doc = DBEventDoc(event_name="ap", tier=0)
		db_job_doc.add_run_id(run_id)

		# Forward jobId to ingester instance 
		# (will be inserted in the transient documents)
		ingester.set_log_id(run_id)

		# Add current run_id to channel specific rejected log saver
		for t0_chan in self.t0_channels:
			t0_chan.rejected_log_handler.set_run_id(run_id)


		# divers
		########

		# Create array
		filter_results = len(self.t0_channels) * [None]

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
			'matches': {}
		}

		dur_stats['allFilters'].fill(np.nan)
		for i, channel in self.chan_enum:
			# dur_stats['filters'][channel.str_name] records filter performances
			# nan will remain only if exception occur for particular alerts
			dur_stats['filters'][channel.str_name] = np.empty(iter_max)
			dur_stats['filters'][channel.str_name].fill(np.nan)
			count_stats['matches'][channel.str_name] = 0

		# The (standard) ingester will update DB insert operations
		if hasattr(ingester, "set_stats_dict"):
			ingester.set_stats_dict(
				dur_stats, count_stats['dbUpd']
			)

		# Save pre run time
		pre_run = time()

		# Accepts and execute pymongo.operations
		with DBUpdatesBuffer(run_id, self.logger).run_in_thread() as updates_buffer:

			chan_names = [chan.str_name for chan in self.t0_channels]

			# Process alerts
			################

			self.logger.debug("#######     Processing alerts     #######")

			# Iterate over alerts
			for alert_content in self.input_setup.get_alert_supplier(alert_loader):

				# Associate upcoming log entries with the current transient id
				tran_id = alert_content['tran_id']

				# Create AmpelAlert instance
				ampel_alert = AmpelAlert(
					tran_id, alert_content['ro_pps'], alert_content['ro_uls']
				)

				# Update cumul stats
				pps_loaded += len(alert_content['pps'])
				if alert_content['uls'] is not None:
					uls_loaded += len(alert_content['uls'])

				# stats
				all_filters_start = time()

				extra = {
					'tranId': tran_id, 
					'alertId': alert_content['alert_id']
				}

				# Loop through initialized channels
				for i, channel in self.chan_enum:

					try:

						# stats
						per_filter_start = time()

						# Apply filter (returns None in case of rejection or t2 runnable ids in case of match)
						filter_results[i] = channel.filter_func(ampel_alert)

						# stats
						dur_stats['filters'][channel.str_name][iter_count] = time() - per_filter_start

						# Log minimal entry if channel did not log anything
						if not channel.rec_buf_hdlr.buffer:
							channel.logger.info(None, extra)

						# Filter rejected alert
						if filter_results[i] is None:

							# "live" autocomplete activated for this channel
							if self.live_ac and self.chan_auto_complete[i] and tran_id in tran_ids_before[i]:

								# Main logger feedback
								self.logger.info(None, 
									extra={
										**extra, 'channel': channel.name, 'autoComplete': True
									}
								)

								# Update counter
								count_stats['matches'][channel.str_name] += 1

								# Use default t2 units as filter results
								filter_results[i] = channel.t2_units
						
								# Rejected logs go to separate collection
								channel.rec_buf_hdlr.forward(
									channel.rejected_logger, extra={**extra, 'autoComplete': True}
								)

							else:

								# Save possibly existing error to 'main' logs
								if channel.rec_buf_hdlr.has_error:
									channel.rec_buf_hdlr.copy(db_logging_handler, channel.name, extra)

								# Save rejected logs to separate (channel specific) db collection
								channel.rec_buf_hdlr.forward(channel.rejected_logger, extra=extra)

						# Filter accepted alert
						else:

							# Update counter
							count_stats['matches'][channel.str_name] += 1

							# enables log concatenation across different loggers
							if self.embed:
								AmpelLogger.current_logger = None 
								AmpelLogger.aggregation_ok = True

							# Write log entries to main logger
							channel.rec_buf_hdlr.forward(self.logger, channel.name, extra)

					# Unrecoverable errors
					except (PyMongoError, AmpelLoggingError, DBUpdateError) as e:
						print("%s: abording run() procedure" % e.__class__.__name__)
						self.report_alertproc_exception(e, run_id, alert_content)
						raise e

					# Tolerable errors
					except Exception as e:
						if raise_exc:
							raise
						channel.rec_buf_hdlr.forward(db_logging_handler, extra=extra)
						self.report_alertproc_exception(
							e, run_id, alert_content, include_photo=True,
							extra={'section': 'filter', 'channel': channel.name}
						)

				# time required for all filters
				dur_stats['allFilters'][iter_count] = time() - all_filters_start

				if any(t2 is not None for t2 in filter_results):

					# stats
					ingested_count += 1

					# TODO: build tran_id <-> alert_id map (replayability)
					#processed_alert[tran_id]
					try: 

						ingester_start = time()

						# Ingest alert
						db_updates = ingester.ingest(
							tran_id, alert_content['pps'], 
							alert_content['uls'], 
							filter_results
						)

						dur_stats['preIngestTime'].append(time()-ingester_start)
						updates_buffer.add_updates(db_updates)

					except AmpelLoggingError as e:
						print("AmpelLoggingError: abording run() procedure")
						self.report_alertproc_exception(e, run_id, alert_content, include_photo=False)
						raise e

					except DBUpdateError as e:
						print("DBUpdateError: abording run() procedure")
						# Flush loggers (possible Exceptions handled by method)
						self.conclude_logging(iter_count, db_logging_handler, full_console_logging)
						raise e

					except Exception as e:
						self.report_alertproc_exception(
							e, run_id, alert_content, filter_results,
							extra={'section': 'ingest'}, include_photo=False
						)
				else:

					# If all channels reject this alert, no log entries goes into
					# the main logs collection sinces those are redirected to Ampel_rej.
					# So we add a notification manually. For that, we don't use self.logger 
					# cause rejection messages were alreary logged into the console 
					# by the StreamHandler in channel specific RecordsBufferingHandler instances. 
					# So we address directly db_logging_handler, and for that, we create
					# a LogRecord manually.
					lr = LogRecord(None, INFO, None, None, None, None, None)
					lr.extra = {
						'tranId': tran_id,
						'alertId': alert_content['alert_id'],
						'allRejected': True,
						'channels': chan_names
					}
					db_logging_handler.handle(lr)

				iter_count += 1

				if iter_count == iter_max:
					self.logger.info("Reached max number of iterations")
					break

		# Save post run time
		post_run = time()

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

				if hasattr(ingester, 'count_dict'):
					count_stats['dbUpd'] = ingester.count_dict
					# Cleaner structure
					count_stats['ppReprocs'] = count_stats['dbUpd'].pop('ppReprocs')


				# Compute mean time & std dev in microseconds
				#############################################

				self.logger.info("Computing job stats")

				dur_stats['preIngestTime'] = self.compute_stat(dur_stats['preIngestTime'])

				# For ingest metrics
				for time_metric in ('dbBulkTime', 'dbPerOpMeanTime'):
					for col in ("Photo", "Blend", "Tran"):
						key = time_metric+col
						if updates_buffer.metrics[key]: 
							dur_stats[key] = self.compute_stat(updates_buffer.metrics[key])

				# per chan filter metrics
				len_non_nan = lambda x: iter_max - np.count_nonzero(np.isnan(x))
				for key in [channel.str_name for channel in self.t0_channels]:
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
				dur_stats['apPostLoop'] = int(time() - post_run)

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

			self.logger.shout(
				"Alert processing completed (time required: %is)" % 
				int(time() - run_start)
			)

			# Flush loggers
			self.conclude_logging(
				iter_count, db_logging_handler, full_console_logging
			)

			db_job_doc.publish()

		except Exception as e:

			# Try to insert doc into trouble collection (raises no exception)
			# Possible exception will be logged out to console in any case
			LoggingUtils.report_exception(
				self.logger, e, tier=0, run_id=db_logging_handler.get_run_id()
			)
			
		# Return number of processed alerts
		return iter_count


	def conclude_logging(self, iter_count, db_logging_handler, full_console_logging):
		"""
		:param int iter_count:
		:param DBLoggingHandler db_logging_handler:
		:param bool full_console_logging:
		:returns: None
		:raises: None
		"""

		try:

			# Restore console logging settings
			if not full_console_logging:
				self.logger.louden_console_loggers()

			# Flush loggers
			if iter_count > 0:

				# Main logs logger. This can raise exceptions
				db_logging_handler.flush()

				# Flush channel-specific rejected logs
				for t0_chan in self.t0_channels:
					t0_chan.rejected_log_handler.flush()

			else:
				db_logging_handler.purge()

			# Remove DB logging handler
			self.logger.removeHandler(db_logging_handler)

		except Exception as e:

			# Try to insert doc into trouble collection (raises no exception)
			# Possible exception will be logged out to console in any case
			LoggingUtils.report_exception(
				self.logger, e, tier=0, run_id=db_logging_handler.get_run_id()
			)


	def compute_stat(self, seq, mean=np.mean, std=np.std):
		"""
		"""

		if np.all(np.isnan(seq)):
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

		col = AmpelDB.get_collection('tran')
		tran_ids = len(self.t0_channels) * [None]

		# Loop through activated channels
		for i, channel in self.chan_enum:

			if self.chan_auto_complete[i]:

				# Build set of transient ids for this channel
				tran_ids[i] = {
					el['_id'] for el in col.find(
						{'channels': channel.name},
						{'_id': 1}
					)
				}

		return tran_ids


	def report_alertproc_exception(
		self, arg_e, run_id, alert_content, filter_results=None, extra=None, include_photo=True
	):
		"""
		:param Exception arg_e:
		:param int run_id:
		:param dict alert_content:
		:param list filter_results:
		:param dict extra: optional extra key/value fields to add to 'trouble' doc
		:param bool include_photo: whether to include alert content into 'trouble' doc
		:rtype: bool
		:returns: True on error (doc could not be published), otherwise False
		:raises: None
		"""

		info={
			'tranId': alert_content.get('alert_id'),
			'alertId': alert_content.get('alert_id')
		}

		if include_photo:
			info['alertPPS'] = alert_content.get('pps')
			info['alertULS'] = alert_content.get('uls')

		if extra:
			for k in extra.keys():
				info[k] = extra[k]

		if filter_results:
			extra['channels'] = [
				channel.str_name 
				for i, channel in self.chan_enum 
				if filter_results[i]
			]

		# Try to insert doc into trouble collection (raises no exception)
		# Possible exception will be logged out to console in any case
		LoggingUtils.report_exception(
			self.logger, arg_e, tier=0, run_id=run_id, info=info
		)
