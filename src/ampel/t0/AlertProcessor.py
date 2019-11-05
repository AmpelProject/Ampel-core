#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/t0/AlertProcessor.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 10.10.2017
# Last Modified Date: 03.11.2019
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import numpy as np
from time import time
from io import IOBase
from pydantic import BaseModel, validator
from logging import LogRecord, INFO
from pymongo.errors import PyMongoError
from typing import Sequence, List, Dict, Union, Any, Iterable, Tuple, Set, Callable

from ampel.logging.AmpelLogger import AmpelLogger
from ampel.logging.LoggingUtils import LoggingUtils
from ampel.logging.T0ConsoleFormatter import T0ConsoleFormatter
from ampel.logging.LogsBufferingHandler import LogsBufferingHandler
from ampel.logging.DBLoggingHandler import DBLoggingHandler
from ampel.logging.DBEventDoc import DBEventDoc
from ampel.logging.AmpelLoggingError import AmpelLoggingError
from ampel.db.AmpelDB import AmpelDB
from ampel.t0.APFilter import APFilter
from ampel.t0.load.AlertSupplier import AlertSupplier
from ampel.t0.ingest.DBUpdatesBuffer import DBUpdatesBuffer
from ampel.base.AmpelAlert import AmpelAlert
from ampel.config.AmpelConfig import AmpelConfig
from ampel.common.GraphiteFeeder import GraphiteFeeder
from ampel.core.flags.LogRecordFlag import LogRecordFlag
from ampel.core.AmpelUnitLoader import AmpelUnitLoader
from ampel.abstract.AbsAmpelProcessor import AbsAmpelProcessor
from ampel.abstract.AbsAlertIngester import AbsAlertIngester
from ampel.model.t0.APChanData import APChanData
from ampel.model.AmpelBaseModel import AmpelBaseModel

class AlertProcessor(AbsAmpelProcessor):
	""" 
	Class handling the processing of alerts (T0 level).
	For each alert, following tasks are performed:
	* Load the alert
	* Filter alert based on the configured T0 filter
	* Ingest alert based on the configured ingester
	"""

	iter_max = 50000

	class InitConfig(AmpelBaseModel):
		""" 
		:param publish_stats: publish performance metrics:
		- graphite: send t0 metrics to graphite (graphite server must be defined in ampel_config)
		- processDoc: include t0 metrics in the process event document which is written into the DB

		:param raise_exc: whether the AP should raise Exceptions rather than catching them (default false)

		:param db_logging: whether to save log entries (and the corresponding process doc) into the DB

		:param single_rej_col: 
		- False: rejected logs are saved in channel specific collections
		 (collection name equals channel name)
		- True: rejected logs are saved in a single collection called 'logs'

		:param run_type: LogRecordFlag.SCHEDULED_RUN or LogRecordFlag.MANUAL_RUN

		:param full_console_logging: If false, the logging level of the stdout streamhandler 
		associated with the logger will be set to WARN during the execution of this method
		(it will be reverted to DEBUG before return)

		:param log_format: 'compact' (saves RAM by reducing the number of indexed document) or standard. \
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
		channel: Sequence[APChanData]
		publish_stats: Sequence[str] = ('graphite', 'processDoc')
		raise_exc: bool = False
		log_line_nbr: bool = False
		single_rej_col: bool = False
		full_console_logging: bool = True
		log_format: str = "compact"
		run_type: LogRecordFlag = LogRecordFlag.SCHEDULED_RUN

		@validator('channel', pre=True, whole=True)
		def cast_to_list(cls, v):
			if isinstance(v, dict):
				return (v, )
			return v


	@classmethod
	def from_process(cls, ampel_config: AmpelConfig, process_name: str):
		""" 
		Convenience methods that instantiates an AP instance using the configuration
		from a given T0 process
		example: AlertProcessor.by_proc_name(ampel_config, ampel_db, "t0_nersc_delayed")
		"""
		cls_name = ampel_config.get(
			f"t0.process.{process_name}.processor.className"
		)

		if cls_name != cls.__class__:
			raise ValueError(
				f"The request process should be invoked using class {cls_name}"
			)

		return cls(
			ampel_config, ampel_config.get(
				f"t0.process.{process_name}.processor.initConfig.channel"
			)
		)


	# pylint: disable=super-init-not-called
	# Using forward reference type hinting for init_config
	def __init__(self, ampel_config: AmpelConfig, init_config: 'AlerProcessor.InitConfig'):
		"""
		"""
	
		self._init_config = init_config if isinstance(init_config, BaseModel) else self.InitConfig(**init_config)
		self._ampel_db = AmpelDB(ampel_config)
		self._alert_ingester = None

		# Setup logger
		self.logger = AmpelLogger.get_unique_logger(
			formatter = T0ConsoleFormatter(
				line_number = self._init_config.log_line_nbr
			)
		)

		lbh = LogsBufferingHandler(tier=0)
		self.logger.addHandler(lbh)
		self._embed = self._init_config.log_format == "compact"

		self.logger.info("Setting up new AlertProcessor instance")

		ampel_unit_loader = AmpelUnitLoader(ampel_config, 0)

		# Load channel defined T0 procs
		self._ap_filters = [
			# Create APFilter instances (instantiates channel filter and loggers)
			APFilter(
				self._ampel_db, ampel_unit_loader, ap_chan_data, 
				self.logger, self._init_config.log_line_nbr, 
				self._embed, self._init_config.single_rej_col
			)
			for ap_chan_data in self._init_config.channel
		]

		if len(self._ap_filters) == 1 and self._init_config.log_format == "compact":
			self.logger.warning("You should not use log_format='compact' when working with only one T0 process")

		if self._init_config.single_rej_col:
			self._ampel_db.enable_rejected_collections(['rejected'])
		else:
			self._ampel_db.enable_rejected_collections(
				[ap_filter.chan_str for ap_filter in self._ap_filters]
			)

		# Shortcuts
		self._filter_enum = list(enumerate(self._ap_filters))
		self._chan_auto_complete = len(self._ap_filters) * [False]
		self._live_ac = False

		# Live auto-complete
		for i, ap_filter in self._filter_enum:
			if ap_filter.auto_complete == "live":
				self._live_ac = True
				self._chan_auto_complete[i] = True

		# Robustness
		if len(self._ap_filters) == 0:
			raise ValueError("No T0 process loaded, please check your config")

		self.logger.info("AlertProcessor setup completed")
		self.logger.removeHandler(lbh)
		self._log_headers = lbh.log_dicts

		# Graphite
		if "graphite" in self._init_config.publish_stats:
			self._gfeeder = GraphiteFeeder(
				ampel_config.get('resource.graphite.default'),
				autoreconnect = True
			)


	def set_alert_ingester(self, alert_ingester: AbsAlertIngester) -> None:
		"""
		:param alert_ingester: sets ingester instance. 
		"""
		self._alert_ingester = alert_ingester
		self._alert_ingester.set_config(
			self._init_config.channel
		)


	def set_alert_supplier(self, alert_supplier: AlertSupplier) -> None:
		"""
		:param alert_supplier: sets supplier instance. 
		"""
		self.alert_supplier = alert_supplier


	def source_alert_supplier(self, alert_loader: Iterable[IOBase]) -> None:
		"""
		:param alert_loader: sets ingester instance. 
		"""
		self.alert_supplier.set_alert_source(alert_loader)


	def process_alerts(self, alert_loader: Iterable[IOBase]) -> None:
		""" 
		shortcut method: process all alerts from a given loader until its dries out
		:param alert_loader: iterable returning alert payloads
		"""
		self.source_alert_supplier(alert_loader)
		processed_alerts = self.iter_max
		while processed_alerts == AlertProcessor.iter_max:
			processed_alerts = self.run()
		self.logger.info("Alert loader dried out")


	def run(self) -> int:
		"""
		Run alert processing using the provided alert_loader
		:raises: LogFlushingError, PyMongoError
		"""

		if not self.alert_supplier or not self.alert_supplier.ready():
			raise ValueError("Alert supplier not set or not sourced")

		if not self._alert_ingester:
			raise ValueError("Please define an alert ingester (method set_alert_ingester)")

		# Save current time to later evaluate processing time
		run_start = time()

		# Create DB logging handler instance (logging.Handler child class)
		# This class formats, saves and pushes log records into the DB
		db_logging_handler = DBLoggingHandler(
			self._ampel_db, LogRecordFlag.T0 | LogRecordFlag.CORE | 
			self._init_config.run_type
		)

		db_logging_handler.add_headers(self._log_headers)
		run_id = db_logging_handler.get_run_id()

		# Add db logging handler to the logger stack of handlers 
		self.logger.handlers.insert(0, db_logging_handler)
		if not self._init_config.full_console_logging:
			self.logger.quieten_console_loggers()

		self.logger.shout("Starting")

		# Add new doc in the 'events' collection
		db_proc_doc = DBEventDoc(
			self._ampel_db, event_name="ap", tier=0
		)
		db_proc_doc.add_run_id(run_id)

		# Forward jobId to ingester instance 
		# (will be inserted in the transient documents)
		self._alert_ingester.set_log_id(run_id)

		# Add current run_id to channel specific rejected log saver
		for ap_filter in self._ap_filters:
			ap_filter.rejected_log_handler.set_run_id(run_id)

		# Create array
		filter_results = len(self._ap_filters) * [None]

		# Save ampel 'state' and get list of tran ids required for autocomplete
		if self._live_ac:
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
			'dbBulkTimeStock': [],
			'dbBulkTimeT0': [],
			'dbBulkTimeT1': [],
			'dbBulkTimeT2': [],
			'dbPerOpMeanTimeStock': [],
			'dbPerOpMeanTimeT0': [],
			'dbPerOpMeanTimeT1': [],
			'dbPerOpMeanTimeT2': [],
			'allFilters': np.empty(iter_max),
			'filters': {}
		}

		# Count statistics (incrementing integer values)
		count_stats = {
			'alerts': 0, 'ingested': 0, 'pps': 0,
			'uls': 0, 'matches': {}
		}

		dur_stats['allFilters'].fill(np.nan)
		for i, ap_filter in self._filter_enum:
			# dur_stats['filters'][ap_filter.chan_str] records filter performances
			# nan will remain only if exception occur for particular alerts
			dur_stats['filters'][ap_filter.chan_str] = np.empty(iter_max)
			dur_stats['filters'][ap_filter.chan_str].fill(np.nan)
			count_stats['matches'][ap_filter.chan_str] = 0

		# The (standard) ingester will update DB insert operations
		if hasattr(self._alert_ingester, "set_stats_dict"):
			self._alert_ingester.set_stats_dict(
				dur_stats, count_stats['dbUpd']
			)

		# Save pre run time
		pre_run = time()

		# Accepts and execute pymongo.operations
		updates_buffer = DBUpdatesBuffer(self, run_id, self.logger)
		self._cancel_run = False

		chan_names = [
			ap_filter.chan_str 
			for ap_filter in self._ap_filters
		]

		# Process alerts
		################

		self.logger.debug("#######     Processing alerts     #######")

		# Iterate over alerts
		for alert_content in self.alert_supplier:

			if self._cancel_run:
				print("Abording run() procedure")
				# Flush loggers (possible Exceptions handled by method)
				self._conclude_logging(iter_count, db_logging_handler)
				return iter_count

			# Associate upcoming log entries with the current transient id
			tran_id = alert_content['tran_id']

			# Create AmpelAlert instance
			ampel_alert = AmpelAlert(
				tran_id, alert_content['ro_pps'], 
				alert_content['ro_uls']
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
			for i, ap_filter in self._filter_enum:

				try:

					# stats
					per_filter_start = time()

					# Apply filter (returns None in case of rejection or t2 runnable ids in case of match)
					filter_results[i] = ap_filter.filter_func(ampel_alert)

					# stats
					dur_stats['filters'][ap_filter.chan_str][iter_count] = time() - per_filter_start

					# Log minimal entry if channel did not log anything
					if not ap_filter.rec_buf_hdlr.buffer:
						ap_filter.logger.info(None, extra)

					# Filter rejected alert
					if filter_results[i] is None:

						# "live" autocomplete activated for this channel
						if (
							self._live_ac and 
							self._chan_auto_complete[i] and 
							tran_id in tran_ids_before[i]
						):

							# Main logger feedback
							self.logger.info(None, 
								extra={
									**extra, 
									'channel': ap_filter.channel, 
									'autoComplete': True
								}
							)

							# Update counter
							count_stats['matches'][ap_filter.chan_str] += 1

							# Use default t2 units as filter results
							filter_results[i] = ap_filter.t2_units
					
							# Rejected logs go to separate collection
							ap_filter.rec_buf_hdlr.forward(
								ap_filter.rejected_logger, 
								extra={**extra, 'autoComplete': True}
							)

						else:

							# Save possibly existing error to 'main' logs
							if ap_filter.rec_buf_hdlr.has_error:
								ap_filter.rec_buf_hdlr.copy(
									db_logging_handler, ap_filter.channel, extra
								)

							# Save rejected logs to separate (channel specific) db collection
							ap_filter.rec_buf_hdlr.forward(
								ap_filter.rejected_logger, extra=extra
							)

					# Filter accepted alert
					else:

						# Update counter
						count_stats['matches'][ap_filter.chan_str] += 1

						# enables log concatenation across different loggers
						if self._embed:
							AmpelLogger.current_logger = None 
							AmpelLogger.aggregation_ok = True

						# Write log entries to main logger
						ap_filter.rec_buf_hdlr.forward(
							self.logger, ap_filter.channel, extra
						)

				# Unrecoverable (logging related) errors
				except (PyMongoError, AmpelLoggingError) as e:
					print("%s: abording run() procedure" % e.__class__.__name__)
					self._report_alertproc_exception(e, run_id, alert_content)
					updates_buffer.push_updates() # try to push these
					raise e

				# Tolerable errors (could be an error from a contributed filter)
				except Exception as e:
			
					ap_filter.rec_buf_hdlr.forward(
						db_logging_handler, extra=extra
					)

					self._report_alertproc_exception(
						e, run_id, alert_content, include_photo=True,
						extra={'section': 'filter', 'channel': ap_filter.channel}
					)

					if self._init_config.raise_exc:
						updates_buffer.push_updates() # try to push these
						raise e

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
					db_updates = self._alert_ingester.ingest(
						tran_id, alert_content['pps'], 
						alert_content['uls'], 
						filter_results
					)

					dur_stats['preIngestTime'].append(time()-ingester_start)
					updates_buffer.add_updates(db_updates)

				except (PyMongoError, AmpelLoggingError) as e:

					print("%s: abording run() procedure" % e.__class__.__name__)
					self._report_alertproc_exception(
						e, run_id, alert_content, 
						include_photo=False
					)
					updates_buffer.push_updates() # try to push these
					raise e

				except Exception as e:

					self._report_alertproc_exception(
						e, run_id, alert_content, filter_results,
						extra={'section': 'ingest'}, include_photo=False
					)

					if self._init_config.raise_exc:
						updates_buffer.push_updates() # try to push these
						raise e
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

			updates_buffer.ap_push_updates()

		# Push all remaining DB updates
		updates_buffer.close()

		# Save post run time
		post_run = time()

		# Post run section
		try: 

			# Optional autocomplete
			if self._live_ac:

				tran_ids_after = self.get_tran_ids()
	
				# Check post auto-complete
				for i, ap_filter in self._filter_enum:
					if isinstance(tran_ids_after[i], set):
						auto_complete_diff = tran_ids_after[i] - tran_ids_before[i]
						if auto_complete_diff:
							# TODO: implement fetch from archive
							pass 
	
			if self._init_config.publish_stats is not None and iter_count > 0:
	
				# include loop counts
				count_stats['alerts'] = iter_count
				count_stats['ingested'] = ingested_count
				count_stats['pps'] = pps_loaded
				count_stats['uls'] = uls_loaded

				if hasattr(self._alert_ingester, 'count_dict'):
					count_stats['dbUpd'] = self._alert_ingester.count_dict
					# Cleaner structure
					count_stats['ppReprocs'] = count_stats['dbUpd'].pop('ppReprocs')


				# Compute mean time & std dev in microseconds
				#############################################

				self.logger.info("Computing job stats")

				dur_stats['preIngestTime'] = self._compute_stat(
					dur_stats['preIngestTime']
				)

				# For ingest metrics
				for time_metric in ('dbBulkTime', 'dbPerOpMeanTime'):
					for col in ("Stock", "T0", "T1", "T2"):
						key = time_metric+col
						if updates_buffer.metrics[key]: 
							dur_stats[key] = self._compute_stat(
								updates_buffer.metrics[key]
							)

				# per chan filter metrics
				for key in chan_names:
					dur_stats['filters'][key] = self._compute_stat(
						dur_stats['filters'][key], mean=np.nanmean, std=np.nanstd
					)

				# all filters metric
				dur_stats['allFilters'] = self._compute_stat(
					dur_stats['allFilters'], mean=np.nanmean, std=np.nanstd
				)

				# Durations in seconds
				dur_stats['apPreLoop'] = int(pre_run - run_start)
				dur_stats['apMain'] = int(post_run - pre_run)
				dur_stats['apPostLoop'] = int(time() - post_run)

				# Publish metrics to graphite
				if "graphite" in self._init_config.publish_stats:
					self.logger.info("Sending stats to Graphite")
					self._gfeeder.add_stats_with_mean_std(
						{
							"count": count_stats,
							"duration": dur_stats
						},
						prefix="t0"
					)
					self._gfeeder.send()

				# Publish metrics into document in collection 'runs'
				if "processDoc" in self._init_config.publish_stats:
					db_proc_doc.set_event_info(
						{
							'metrics': {
								"count": count_stats,
								"duration": dur_stats
							}
						}
					)

			self.logger.shout(
				f"Processing completed (time required: {int(time() - run_start)}s)"
			)

			# Flush loggers
			self._conclude_logging(iter_count, db_logging_handler)

			db_proc_doc.publish()

		except Exception as e:

			# Try to insert doc into trouble collection (raises no exception)
			# Possible exception will be logged out to console in any case
			LoggingUtils.report_exception(
				self._ampel_db, self.logger, tier=0, exc=e,
				run_id=db_logging_handler.get_run_id()
			)
			
		# Return number of processed alerts
		return iter_count


	def set_cancel_run(self) -> None:
		"""
		Cancels current processing of alerts
		(when DB becomes unresponsive for example).
		This is an indirect method because DB updates 
		are pushed asyncronously (by threads)
		"""
		self._cancel_run = True


	def _conclude_logging(self, iter_count: int, db_logging_handler: DBLoggingHandler) -> None:
		"""
		:raises: None
		"""

		try:

			# Restore console logging settings
			if not self._init_config.full_console_logging:
				self.logger.louden_console_loggers()

			# Flush loggers
			if iter_count > 0:

				# Main logs logger. This can raise exceptions
				db_logging_handler.flush()

				# Flush channel-specific rejected logs
				for ap_filter in self._ap_filters:
					ap_filter.rejected_log_handler.flush()

			else:
				db_logging_handler.purge()

			# Remove DB logging handler
			self.logger.removeHandler(db_logging_handler)

		except Exception as e:

			# Try to insert doc into trouble collection (raises no exception)
			# Possible exception will be logged out to console in any case
			LoggingUtils.report_exception(
				self._ampel_db, self.logger, tier=0,  exc=e,
				run_id=db_logging_handler.get_run_id()
			)


	def get_tran_ids(self) -> Set[Union[int, str]]:
		"""
		:returns: array whose length equals len(self._ap_filters), possibly containing sets of transient ids.
		If channel[i] is the channel with index i wrt the list of channels 'self._ap_filters', 
		and if channel[i] was configured to make use of the 'live' auto_complete feature, 
		then tran_ids[i] will hold a {set} of transient ids listing all known 
		transients currently available in the DB for this particular channel.
		Otherwise, tran_ids_before[i] will be None
		"""

		col = self._ampel_db.get_collection('stock')
		tran_ids = len(self._ap_filters) * [None]

		# Loop through activated channels
		for i, ap_filter in self._filter_enum:

			if self._chan_auto_complete[i]:

				# Build set of transient ids for this channel
				tran_ids[i] = {
					el['_id'] for el in col.find(
						{'channels': ap_filter.channel},
						{'_id': 1}
					)
				}

		return tran_ids


	def _report_alertproc_exception(
		self, arg_e: Exception, run_id: int, alert_content: Dict[str, Any], 
		filter_results: List = None, extra: Dict[str, Any] = None, include_photo: bool = True
	) -> None:
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
			'alId': alert_content.get('tran_id'),
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
				ap_filter.chan_str for i, ap_filter in self._filter_enum 
				if filter_results[i]
			]

		# Try to insert doc into trouble collection (raises no exception)
		# Possible exception will be logged out to console in any case
		LoggingUtils.report_exception(
			self._ampel_db, self.logger, tier=0, 
			exc=arg_e, run_id=run_id, info=info
		)


	@staticmethod
	def _compute_stat(
		seq, mean: Callable[[Sequence], Sequence] = np.mean, 
		std: Callable[[Sequence], Sequence] = np.std
	) -> Tuple[int, int]:
		"""
		Returns mean time & std dev in microseconds
		"""
		if np.all(np.isnan(seq)):
			return (0, 0)

		# mean time & std dev in microseconds
		return (
			int(round(mean(seq) * 1000000)),
			int(round(std(seq) * 1000000)) 
		)
