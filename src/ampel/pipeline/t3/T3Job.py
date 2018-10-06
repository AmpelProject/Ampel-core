#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/t3/T3Job.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 26.02.2018
# Last Modified Date: 30.09.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import logging
from datetime import datetime
from pymongo import UpdateMany
from itertools import islice

from ampel.pipeline.db.query.QueryMatchTransients import QueryMatchTransients
from ampel.pipeline.db.query.QueryLatestCompound import QueryLatestCompound
from ampel.pipeline.db.query.QueryRunsCol import QueryRunsCol
from ampel.pipeline.db.DBContentLoader import DBContentLoader
from ampel.pipeline.db.AmpelDB import AmpelDB
from ampel.pipeline.logging.DBLoggingHandler import DBLoggingHandler
from ampel.pipeline.logging.AmpelLogger import AmpelLogger
from ampel.pipeline.logging.LoggingUtils import LoggingUtils
from ampel.pipeline.common.AmpelUtils import AmpelUtils
from ampel.pipeline.t3.TimeConstraint import TimeConstraint
from ampel.base.flags.TransientFlags import TransientFlags
from ampel.core.flags.AlDocType import AlDocType
from ampel.core.flags.FlagUtils import FlagUtils


class T3Job:
	"""
	"""

	def __init__(
		self, job_config, central_db=None, logger=None, 
		db_logging=True, full_console_logging=True
	):
		""" 
		:param job_config: instance of ampel.pipeline.config.t3.T3JobConfig

		:param str central_db: Use provided DB name rather than Ampel default database ('Ampel')

		:param logger:\n
		-> If None, a new logger using a DBLoggingHandler will be created, which means a new 
		log document will be inserted into the 'logs' collection of the central db.
		-> If you provide a logger, please note that it will *NOT* be changed in any way, 
		in particular: no DBLoggingHandler will be added, which means that no DB logging will occur.

		:param bool full_console_logging: If false, the logging level of the stdout streamhandler 
		associated with the logger will be set to WARN.
		"""
		
		# Optional override of AmpelConfig defaults
		if central_db is not None:
			AmpelDB.set_central_db_name(central_db)

		self.col_tran = AmpelDB.get_collection('main')
		self.col_jobs = AmpelDB.get_collection('jobs')

		if logger is None:

			# Create logger
			logger = AmpelLogger.get_logger()

			if db_logging:
				# Create DB logging handler instance (logging.Handler child class)
				# This class formats, saves and pushes log records into the DB
				self.db_logging_handler = DBLoggingHandler(tier=3)

				# Add db logging handler to the logger stack of handlers 
				logger.addHandler(self.db_logging_handler)

		self.logger = logger
		self.job_config = job_config
		self.global_info = None
		self.exec_params = {}

		if not full_console_logging:
			self.logger.quieten_console_logger(self.logger)

		# T3 job not requiring any prior transient loading 
		if job_config.get('transients.select') is not None:

			self.exec_params = {
				'channels': job_config.get('transients.select.channels'),
				'state_op': job_config.get("transients.load.state"),
				't2s': job_config.get("transients.load.t2s"),
				'docs': FlagUtils.list_flags_to_enum_flags(
					[job_config.get('transients.load.docs')], AlDocType
				),
				'created': TimeConstraint(
					job_config.get('transients.select.created')
				),
				'modified': TimeConstraint(
					job_config.get('transients.select.modified')
				),
				'with_flags': FlagUtils.list_flags_to_enum_flags(
					job_config.get('transients.select.withFlags'), 
					TransientFlags
				),
				'without_flags': FlagUtils.list_flags_to_enum_flags(
					job_config.get('transients.select.withoutFlags'), 
					TransientFlags
				),
				'chunk': job_config.get('transients.chunk'),
				'verbose': job_config.get("transients.load.verbose"),
				'debug': job_config.get("transients.load.debug")
			}

		# Retrieve number of alerts processed since last run if so whished
		if job_config.get('globalInfo') is True:
			self.global_info = self.get_global_info()


	def get_global_info(self):
		"""
		Retrieves info such as the number of alerts 
		processed since last run of this job
		"""

		# Get datetime of last run
		last_run = AmpelUtils.get_by_path(
			next(
				self.col_jobs.aggregate(
					QueryRunsCol.get_job_last_run(self.job_config.job_name)
				), 
				None
			), 
			'jobs.dt'
		)

		if last_run is not None:

			# Get number of alerts processed since last run
			res = next(
				self.col_jobs.aggregate(
					QueryRunsCol.get_t0_stats(last_run)
				), 
				None
			)

			# Build and return global info
			return {
				'jobName': self.job_config.job_name,
				'lastRun': datetime.fromtimestamp(last_run),
				'processedAlerts': 0 if res is None else res.get('alerts')
			}

		else:

			# Feedback
			self.logger.error(
				"Job %s: global info unavailable" % 
				self.job_config.job_name
			)


	def overwrite_parameter(self, name, value):
		"""
		Overwrites job parameters
		"""
		if not hasattr(self, "exec_params"):
			raise ValueError("No job parameter available")

		if name not in self.exec_params:
			raise ValueError("Unknown attribute: %s" % name)

		self.exec_params[name] = value


	def get_parameters(self):
		"""
		Returns job parameters (dict instance)
		"""
		return getattr(self, "exec_params", None)


	def run(self, update_tran_journal=True, update_run_col=True, report_exceptions=True):
		"""
		:param update_tran_journal: Record the invocation of this job in the journal of each selected transient
		:param update_run_col: Record the invocation of this job in the runs collection
		:param report_exceptions: Catch and report exceptions instead of raising them
		"""

		time_start = datetime.utcnow().timestamp()
		t3_tasks = []
		self.run_id = (
			self.db_logging_handler.get_run_id() 
			if hasattr(self, 'db_logging_handler') else None
		)

		job_descr = "job %s (run id: %s)" % (self.job_config.job_name, self.run_id)

		# Feedback
		self.logger.propagate_log(self.logger, logging.INFO, "Running %s" % job_descr)
		
		try:

			# Check for '$forEach' channel operator
			if next(iter(self.job_config.get_task_configs())).get("select.channels") == "$forEach":
	
				# list of all channels found in matched transients.
				chans = self._get_channels()
	
				# There is no channel-less transient (no channel == no transient)
				if chans is None:
					self.logger.info("No matching transient")
					return
	
				# Set *job* channels parameter to the channel values retrieved dynamically 
				self.overwrite_parameter("channels", chans)
	
				# Create T3 tasks
				for task_config in self.job_config.get_task_configs():
					for channel in chans:
						t3_tasks.append(
							task_config.create_task(
								self.logger, channels=channel, global_info=self.global_info
							)
						)
			else:
	
				# Create T3 tasks
				for task_config in self.job_config.get_task_configs():
					t3_tasks.append(
						task_config.create_task(
							self.logger, 
							channels=(
								task_config.channels if task_config.channels is not None 
								else self.exec_params.get('channels')
							),
							global_info=self.global_info
						)
					)
	
			# T3 job requiring prior transient loading 
			if self.job_config.get('transients.select') is not None:
	
				# Required to get transient info
				dcl = DBContentLoader(
					self.col_tran.database, verbose=self.exec_params['verbose'], 
					debug=self.exec_params['debug'], logger=self.logger
				)
	
				# Job with transient input
				trans_cursor = self.get_selected_transients()
	
				if trans_cursor is not None:
	
					# Set chunk_size to 'number of transients found' if not defined
					chunk_size = self.exec_params['chunk']
	
					# No chunk size == all transients loaded at once
					if chunk_size is None:
						chunk_size = trans_cursor.count()
	
					for tran_register in self.get_tran_data(dcl, trans_cursor, chunk_size):
	
						# Feed each task with transient views
						for t3_task in t3_tasks:
							try:
								t3_task.update(tran_register)
							except:
								if report_exceptions:
									LoggingUtils.report_exception(
										tier=3, run_id=self.run_id, 
										logger=self.logger, info={
											'jobName': self.job_config.job_name,
											'taskName': t3_task.task_config.task_doc.get("name")
										}
									)
								else:
									raise
	
			# For each task, execute done()
			for t3_task in t3_tasks:
	
				try:
					# execute embedded t3unit instance method done()
					specific_journal_entries = t3_task.done()
				except:
					if report_exceptions:
						LoggingUtils.report_exception(
							tier=3, run_id=self.run_id, 
							logger=self.logger, info={
								'jobName': self.job_config.job_name,
								'taskName': t3_task.task_config.task_doc.get("name")
							}
						)
						continue
					else:
						raise

				# Update journal with task related info
				if update_tran_journal:
					self.general_journal_update(t3_task, int(time_start))

				# method done() might return a dict with key: transient id, 
				# value journal entries. In this case, we update the Transient journal 
				# with those entries
				if update_tran_journal and specific_journal_entries:
					# TODO: update journal with t3 unit specific info
					pass
	
			if update_run_col:

				# Record job info into DB
				upd_res = self.col_jobs.update_one(
					{
						'_id': int(
							datetime.today().strftime('%Y%m%d')
						)
					},
					{
						'$push': {
							'jobs': {
								'tier': 3,
								'name': self.job_config.job_name,
								'dt': int(time_start),
								'logs': self.run_id,
								'metrics': {
									'duration': int(
										datetime.utcnow().timestamp() - time_start
									)
								}
							}
						}
					},
					upsert=True
				)

				if upd_res.modified_count == 0 and upd_res.upserted_id is None and report_exceptions:

					# Populate troubles collection
					from inspect import currentframe, getframeinfo
					self._report_error(
						'runs collection update failed', getframeinfo(currentframe()),
						t3_task.get_config('name'), upd_res=upd_res
					)

			# Feedback
			self.logger.propagate_log(
				logging.INFO, "Done running %s" % job_descr
			)

			# Write log entries to DB
			if hasattr(self, 'db_logging_handler'):
				self.db_logging_handler.flush()

		except:

			if report_exceptions:
				LoggingUtils.report_exception(
					tier=3, run_id=self.run_id, 
					logger=self.logger, info={
						'jobName': self.job_config.job_name,
						'logs':  self.run_id,
					}
				)
			else:
				raise

		self.run_id = None


	def _report_error(
		self, message, frameinfo, task_name, upd_res=None, bulk_write_result=None
	):
		"""
		"""

		d = {
			'tier': 3,
			'ampelMsg': message,
			'location': '%s:%s' % (frameinfo.filename, frameinfo.lineno),
			'jobName':  self.job_config.job_name,
			'taskName': task_name,
			'logs': self.run_id,
			'mongoUpdateResult': upd_res.raw_result
		}

		if upd_res is not None:
			d['mongoUpdateResult'] = upd_res.raw_result

		if bulk_write_result is not None:
			d['mongoBulkWriteResult'] = bulk_write_result.bulk_api_result

		# Populate troubles collection
		try:
			AmpelDB.get_collection('troubles').insert_one(d)
		except:
			# Bad luck (possible cause: DB offline)
			self.logger.propagate_log(
				logging.ERROR, 
				"Exception occured while populating 'troubles' collection",
				exc_info=True
			)


	def _get_channels(self):
		"""
		Returns a list of all channels found in the matched transients.
		The list does not contain duplicates.
		None is returned if no matching transient exists
		"""

		query_res = next(
			self.col_tran.aggregate(
				[
					{'$match': self._get_match_criteria()},
					{"$unwind": "$channels"},
					{
						"$group": {
		  					"_id": None,
		        			"channels": {
								"$addToSet": "$channels"
							}
						}
					}
				]
			),
			None
		)

		if query_res is None:
			return None

		return query_res['channels']


	def _get_match_criteria(self):
		"""
		Returns a dict (matching criteria) used for pymongo 
		operations find() or aggregate() operations
		"""

		# Build query for matching transients using criteria defined in job_config
		return QueryMatchTransients.match_transients(
			channels = self.exec_params['channels'],
			time_created = self.exec_params['created'],
			time_modified = self.exec_params['modified'],
			with_flags = self.exec_params['with_flags'],
			without_flags = self.exec_params['without_flags']
		)


	def get_selected_transients(self):
		"""
		Returns a pymongo cursor
		"""

		# Build query for matching transients using criteria defined in job_config
		trans_match_query = self._get_match_criteria()
		self.logger.info("Executing search query: %s" % trans_match_query)

		# Execute 'find transients' query
		trans_cursor = self.col_tran.find(
			trans_match_query, {'_id':0, 'tranId':1}
		).batch_size(100000)
		
		# Count results 
		if trans_cursor.count() == 0:
			self.logger.info("No transient matches the given criteria")
			return None

		self.logger.info("%i transients match search criteria" % trans_cursor.count())

		return trans_cursor


	def get_tran_data(self, db_content_loader, trans_cursor, chunk_size):
		"""
		Yield selected TransientData in chunks of length `chunk_size`
		"""

		self.logger.info("#"*60)
		self.logger.info("Processing chunk")

		# Load ids (chunk_size number of ids)
		for chunked_tran_ids in T3Job.chunk(map(lambda el: el['tranId'], trans_cursor), chunk_size):

			self.logger.info("Loading %i transients " % len(chunked_tran_ids))
			states = None

			# For '$latest' state, the latest compoundid of each transient must be determined
			if self.exec_params['state_op'] == "$latest":

				self.logger.info("Retrieving latest state")

				# See for which ids the fast query cannot be used (save results in a set)
				slow_ids = set(
					el['tranId'] for el in trans_cursor.collection.find(
						{
							'tranId': {
								'$in': chunked_tran_ids
							},
							'alDocType': AlDocType.COMPOUND, 
							'tier': {'$ne': 0}
						},
						{'_id':0, 'tranId':1}
					).batch_size(chunk_size)
				)

				# set of states
				states = set()

				# Channel/Channels must be provided if state is 'latest'
				# Get latest state ** for each channel **
				for channel in AmpelUtils.iter(self.exec_params['channels']):

					# get latest state (fast mode) 
					# Output example:
					# [
					# {
					#   '_id': Binary(b']\xe2H\x0f(\xbf\xca\x0b\xd3\xba\xae\x89\x0c\xb2\xd2\xae', 5), 
					#   'tranId': 1810101034343026   # (ZTF18aaayyuq)
					# },
					# {
					#   '_id': Binary(b'_\xcd\xed\xa5\xe1\x16\x98\x9ai\xf6\xcb\xbd\xe7#FT', 5), 
					#   'tranId': 1810101011182029   # (ZTF18aaabikt)
					# },
					# ...
					# ]
					states.update(
						[
							el['_id'] for el in trans_cursor.collection.aggregate(
								QueryLatestCompound.fast_query(
									slow_ids.symmetric_difference(chunked_tran_ids), 
									channel
								)
							).batch_size(chunk_size)
						]
					)

					# TODO: check result length ?


					# get latest state (general mode) for the remaining transients
					for tran_id in slow_ids:

						# get latest state for single transients using general query
						g_latest_state = next(
							trans_cursor.collection.aggregate(
								QueryLatestCompound.general_query(
									tran_id, project={
										'$project': {'_id':1}
									}
								)
							).batch_size(chunk_size),
							None
						)

						# Robustness
						if g_latest_state is None:
							# TODO: add error flag to transient doc ?
							# TODO: add error flag to job doc
							# TODO: add doc to Ampel_troubles
							self.logger.error(
								"Could not retrieve latest state for transient %s" % 
								tran_id
							)
							continue

						states.add(g_latest_state['_id'])


			# Load ampel TransientData instances with given states
			self.logger.info("Loading transients")
			al_tran_data = db_content_loader.load_new(
				chunked_tran_ids, self.exec_params['channels'], self.exec_params['state_op'], 
				states, self.exec_params['docs'], self.exec_params['t2s']
			)
			
			yield al_tran_data


	def general_journal_update(self, t3_task, int_time_start):
		"""
		updates transient journal with task related info
		"""

		try:

			for channels in t3_task.journal_notes.keys():

				# Update many 
				um = UpdateMany(
					{
						'alDocType': AlDocType.TRANSIENT, 
						'tranId': {'$in': t3_task.journal_notes[channels]}
					},
					{
						'$push': {
							"journal": {
								'tier': 3,
								'dt': int_time_start,
								'jobName':  self.job_config.job_name,
								'taskName': t3_task.get_config('name'),
								'channels': list(channels), # type(channels) is frozenset
								'logs': self.run_id
							}
						}
					}
				)

				self.logger.info("Transient journal update query: %s" % um)
				upd_res = self.col_tran.bulk_write([um])

				# At least one update was not applied
				if upd_res.modified_count != len(t3_task.journal_notes[channels]):
	
					# Populate troubles collection
					from inspect import currentframe, getframeinfo
					self._report_error(
						'transient journal update_many error (len journal upd: %i)' %
						len(t3_task.journal_notes[channels]),
						getframeinfo(currentframe()),
						t3_task.get_config('name'),
						upd_res=upd_res
					)

		except:

			# Populate troubles collection
			LoggingUtils.report_exception(
				tier=3, run_id=self.run_id, 
				logger=self.logger, info={
					'ampelMsg': 'Exception occured in general_journal_update',
					'jobName': self.job_config.job_name,
					'taskName': t3_task.get_config('name'),
					'runId':  self.run_id,
				}
			)


	@staticmethod
	def chunk(iter, chunk_size):
		while True:
			group = list(islice(iter, chunk_size))
			if len(group) > 0:
				yield group
			else:
				break
