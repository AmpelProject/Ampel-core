#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/t3/T3Job.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 26.02.2018
# Last Modified Date: 09.07.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from datetime import datetime, timezone
from pymongo import MongoClient
from itertools import islice
from time import time

from ampel.pipeline.db.query.QueryMatchTransients import QueryMatchTransients
from ampel.pipeline.db.query.QueryLatestCompound import QueryLatestCompound
from ampel.pipeline.db.DBContentLoader import DBContentLoader
from ampel.pipeline.db.AmpelDB import AmpelDB
from ampel.pipeline.logging.DBLoggingHandler import DBLoggingHandler
from ampel.pipeline.logging.LoggingUtils import LoggingUtils
from ampel.pipeline.common.AmpelUtils import AmpelUtils
from ampel.pipeline.t3.TimeConstraint import TimeConstraint
from ampel.base.flags.TransientFlags import TransientFlags
from ampel.core.flags.AlDocTypes import AlDocTypes
from ampel.core.flags.FlagUtils import FlagUtils


class T3Job:
	"""
	"""

	def __init__(self, job_config, central_db=None, logger=None, propagate_logs=False):
		""" 
		'job_config':
		instance of ampel.pipeline.t3.T3JobConfig

		'central_db': string. 
		Use provided DB name rather than Ampel default database ('Ampel')

		'logger': 
		-> If None, a new logger using a DBLoggingHandler will be created, which means a new 
		log document will be inserted into the 'logs' collection of the central db.
		-> If you provide a logger, please note that it will *NOT* be changed in any way, 
		in particular: no DBLoggingHandler will be added, which means that no DB logging will occur.

		'propagate_logs': 
		If this evaluates to true, events logged using 'logger' will be passed to the handlers 
		of higher level (ancestor) loggers, in addition to any handlers attached to 'logger'. 
		If this evaluates to false, logging messages are not passed to the handlers of ancestor loggers.
		"""
		
		# Optional override of AmpelConfig defaults
		if central_db is not None:
			AmpelDB.set_central_db_name(central_db)

		self.tran_col = AmpelDB.get_collection('main')

		if logger is None:

			# Create logger
			logger = LoggingUtils.get_logger()

			# Create DB logging handler instance (logging.Handler child class)
			# This class formats, saves and pushes log records into the DB
			self.db_logging_handler = DBLoggingHandler(
				tier=3, info={"job": job_config.job_name}
			)

			# Add db logging handler to the logger stack of handlers 
			logger.addHandler(self.db_logging_handler)

		logger.propagate = propagate_logs
			
		self.logger = logger
		self.job_config = job_config

		# T3 job not requiring any prior transient loading 
		if job_config.get('input.select') is not None:

			self.exec_params = {
				'channels': job_config.get('input.select.channel(s)'),
				'state_op': job_config.get("input.load.state"),
				't2s': job_config.get("input.load.t2(s)"),
				'docs': FlagUtils.list_flags_to_enum_flags(
					job_config.get('input.load.doc(s)'), AlDocTypes
				),
				'created': TimeConstraint.from_parameters(
					job_config.get('input.select.created')
				),
				'modified': TimeConstraint.from_parameters(
					job_config.get('input.select.modified')
				),
				'with_flags': FlagUtils.list_flags_to_enum_flags(
					job_config.get('input.select.withFlag(s)'), 
					TransientFlags
				),
				'without_flags': FlagUtils.list_flags_to_enum_flags(
					job_config.get('input.select.withoutFlag(s)'), 
					TransientFlags
				),
				'feedback': True,
				'verbose_feedback': True
			}


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


	def run(self):
		"""
		"""

		# TODO: try/catch with Ampel 'troubles' insert

		time_now = time()
		t3_tasks = []

		# Check for '$forEach' channel operator
		if next(iter(self.job_config.get_task_configs())).get("select.channel(s)") == "$forEach":

			# list of all channels found in matched transients.
			chans = self._get_channels()

			# There is no channel less transient -> no channel == no transient
			if chans is None:
				self.logger.info("No matching transient")
				return

			# Set *job* channels parameter to the channel values retrieved dynamically 
			self.overwrite_parameter("channels", chans)

			# Create T3 tasks
			for task_config in self.job_config.get_task_configs():
				for channel in chans:
					t3_tasks.append(task_config.create_task(self.logger, channel=channel))
		else:

			# Create T3 tasks
			for task_config in self.job_config.get_task_configs():
				t3_tasks.append(task_config.create_task(self.logger))

		# T3 job requiring prior transient loading 
		if self.job_config.get('input.select') is not None:

			# Required to load single transients
			dcl = DBContentLoader(self.tran_col.database, verbose=True, logger=self.logger)

			# Job with transient input
			trans_cursor = self.get_selected_transients()

			if trans_cursor is not None:

				# Set chunk_size to 'number of transients found' if not defined
				chunk_size = self.job_config.get('input.chunk')

				# No chunk size == all transients loaded at once
				if chunk_size is None:
					chunk_size = trans_cursor.count()

				for tran_register in self.get_tran_data(dcl, trans_cursor, chunk_size):

					self.logger.info("Processing chunk")

					# Feed each task with transient views
					for t3_task in t3_tasks:
						t3_task.update(tran_register)
			

		# For each task, execute done()
		for t3_task in t3_tasks:

			# execute embedded t3unit instance method done()
			ret = t3_task.done()

			# method done() might return a list of transient ids (integers or strings). 
			# In this case, we update the Transient journal with dict containing date, 
			# channels and name of the task
			if ret:

				mongo_res = self.tran_col.update_many(
					{
						'alDocType': AlDocTypes.TRANSIENT, 
						'tranId': {'$in': ret}
					},
					{
						'$push': {
							"journal": {
								'dt': int(datetime.now(timezone.utc).timestamp()),
								'tier': 3,
								'channels': list(t3_task.channels), # make sure channels is a list
								'taskName': t3_task.get_config('name')
							}
						}
					}
				)

				if mongo_res.raw_result["nModified"] != len(ret):
					pass # populate ampel_troubles !


		# Record job info into DB
		AmpelDB.get_collection('runs').update_one(
			{'_id': int(datetime.today().strftime('%Y%m%d'))},
			{
				'$push': {
					'jobs': {
						'tier': 3,
						'job': self.job_config.job_name,
						'dt': datetime.now(timezone.utc).timestamp(),
						'logs': (
							self.db_logging_handler.get_log_id() if hasattr(self, 'db_logging_handler') 
							else None
						),
						'metrics': {
							'duration': int(time() - time_now)
						}
					}
				}
			},
			upsert=True
		)

		# Write log entries to DB
		if hasattr(self, 'db_logging_handler'):
			self.db_logging_handler.flush()


	def _get_channels(self):
		"""
		Returns a list of all channels found in the matched transients.
		The list does not contain duplicates.
		None is returned if no matching transient exists
		"""

		query_res = next(
			self.tran_col.aggregate(
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
		Returns a dict of pymongo matching criteria 
		used for find() or aggregate() operations
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
		trans_cursor = self.tran_col.find(
			trans_match_query, {'_id':0, 'tranId':1}
		).batch_size(100000)
		
		# Count results 
		if trans_cursor.count() == 0:
			self.logger.info("No transient matches the given criteria")
			return None

		self.logger.info("%i transient(s) match search criteria" % trans_cursor.count())

		return trans_cursor


	def get_tran_data(self, db_content_loader, trans_cursor, chunk_size):
		"""
		Yield selected TransientData in chunks of length `chunk_size`
		"""
		# Load ids (chunk_size number of ids)
		for chunked_tran_ids in T3Job.chunk(map(lambda el: el['tranId'], trans_cursor), chunk_size):

			self.logger.info("Loading %i transient(s) " % len(chunked_tran_ids))
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
							'alDocType': AlDocTypes.COMPOUND, 
							'tier': {'$ne': 0}
						},
						{'_id':0, 'tranId':1}
					).batch_size(chunk_size)
				)

				# set of states
				states = set()

				# Channel/Channels must be provided if state is 'latest'
				# Get latest state ** for each channel **
				channels = self.exec_params['channels']
				for channel in channels if type(channels) is not str else [channels]:

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


			# Load ampel TransientData instances with given state(s)
			self.logger.info("Loading transient(s)")
			al_tran_data = db_content_loader.load_new(
				chunked_tran_ids, self.exec_params['channels'], self.exec_params['state_op'], 
				states, self.exec_params['docs'], self.exec_params['t2s'], 
				self.exec_params['feedback'], self.exec_params['verbose_feedback']
			)
			
			yield al_tran_data
	

	@staticmethod
	def chunk(iter, chunk_size):
		while True:
			group = list(islice(iter, chunk_size))
			if len(group) > 0:
				yield group
			else:
				break
