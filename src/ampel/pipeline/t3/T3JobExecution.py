#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/t3/T3JobExecution.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 26.02.2018
# Last Modified Date: 14.06.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from ampel.pipeline.db.query.QueryMatchTransients import QueryMatchTransients
from ampel.pipeline.db.query.QueryLatestCompound import QueryLatestCompound
from ampel.pipeline.db.DBContentLoader import DBContentLoader
from ampel.pipeline.logging.LoggingUtils import LoggingUtils
from ampel.pipeline.t3.TimeConstraint import TimeConstraint
from ampel.pipeline.common.AmpelUtils import AmpelUtils
from ampel.flags.TransientFlags import TransientFlags
from ampel.flags.AlDocTypes import AlDocTypes
from ampel.flags.FlagUtils import FlagUtils
from datetime import datetime, timezone
from itertools import islice

class T3JobExecution:
	"""
	"""

	def __init__(self, t3_job, logger):
		""" 
		"""

		self.logger = logger
		self.t3_job = t3_job

		# T3 job not requiring any prior transient loading 
		if t3_job.get_config('input.select') is not None:

			self.exec_params = {
				'channels': t3_job.get_config('input.select.channel(s)'),
				'state': t3_job.get_config("input.load.state"),
				't2s': t3_job.get_config("input.load.t2(s)"),
				'docs': FlagUtils.list_flags_to_enum_flags(
					t3_job.get_config('input.load.doc(s)'), AlDocTypes
				),
				'created': TimeConstraint.from_parameters(
					t3_job.get_config('input.select.created')
				),
				'modified': TimeConstraint.from_parameters(
					t3_job.get_config('input.select.modified')
				),
				'with_flags': FlagUtils.list_flags_to_enum_flags(
					t3_job.get_config('input.select.withFlag(s)'), 
					TransientFlags
				),
				'without_flags': FlagUtils.list_flags_to_enum_flags(
					t3_job.get_config('input.select.withoutFlag(s)'), 
					TransientFlags
				),
				'feedback': True,
				'verbose_feedback': True
			}


	def overwrite_job_parameter(self, name, value):
		"""
		"""
		if not hasattr(self, "exec_params"):
			raise ValueError("No job parameter available")
		if name not in self.exec_params:
			raise ValueError("Unknown attribute: %s" % name)
		self.exec_params[name] = value


	def get_job_parameters(self):
		"""
		"""
		return getattr(self, "exec_params", None)


	def run_job(self, central_db=None, al_config=None):
		"""
		central_db: pymongo database 
		al_config: ampel config (dict)
		"""
		# T3 job requiring prior transient loading 
		if self.t3_job.get_config('input.select') is not None:

			if None in (al_config, central_db):
				raise ValueError(
					"Ampel config and central_db parameters are required for jobs requiring transient loading"
				)

			# Required to load single transients
			dcl = DBContentLoader(central_db, al_config, verbose=True, logger=self.logger)

			# Job with transient input
			trans_cursor = self.get_selected_transients(central_db.main)

			if trans_cursor is not None:

				# Set chunk_size to 'number of transients found' if not defined
				chunk_size = self.t3_job.get_chunk()
				if chunk_size is None:
					chunk_size = trans_cursor.count()

				while self.process_chunk(dcl, trans_cursor, chunk_size) == chunk_size:
					self.logger.info("Processing next chunk")

		# For each task, create transientView and run task
		for t3_task in self.t3_job.get_tasks():

			self.logger.info("Running task %s" % t3_task.get_config("name"))
			ret = t3_task.run(self.logger)

			# Update Transient journal with dict containing date, channels and name of the task
			if t3_task.get_config('updateJournal') is True:
				mongo_res = central_db.main.update_many(
					{
						'alDocType': AlDocTypes.TRANSIENT, 
						'tranId': {'$in': ret}
					},
					{
						'$push': {
							"journal": {
								'dt': int(datetime.now(timezone.utc).timestamp()),
								'tier': 3,
								'channels': AmpelUtils.iter( # make sure channels is a list
									t3_task.get_config('select.channel(s)')
								),
								'taskName': t3_task.get_config('name')
							}
						}
					}
				)

				if mongo_res.raw_result["nModified"] != len(ret):
					pass # populate ampel_troubles !

		# Record last time this job was run into DB
		col=central_db['runs']
		col.update_one(
			{'_id': self.t3_job.job_name},
			{
				'$set': {
					'lastRun': datetime.now(timezone.utc).timestamp()
				}
			},
			upsert=True
		)


	def get_selected_transients(self, tran_col):
		"""
		Returns a pymongo cursor
		"""

		# Build query for matching transients using criteria defined in job_config
		trans_match_query = QueryMatchTransients.match_transients(
			channels = self.exec_params['channels'],
			time_created = self.exec_params['created'],
			time_modified = self.exec_params['modified'],
			with_flags = self.exec_params['with_flags'],
			without_flags = self.exec_params['without_flags']
		)

		self.logger.info("Executing search query: %s" % trans_match_query)

		# Execute 'find transients' query
		trans_cursor = tran_col.find(
			trans_match_query, {'_id':0, 'tranId':1}
		).batch_size(100000)
		
		# Count results 
		if trans_cursor.count() == 0:
			self.logger.info("No transient matches the given criteria")
			return None

		self.logger.info("%i transient(s) match search criteria" % trans_cursor.count())

		return trans_cursor


	def process_chunk(self, db_content_loader, trans_cursor, chunk_size):
		"""
		db_content_loader: instance of DBContentLoader
		"""

		# Load ids (chunk_size number of ids)
		chunked_tran_ids = [el['tranId'] for el in islice(trans_cursor, chunk_size)]

		if len(chunked_tran_ids) == 0:
			self.logger.info("Job done")
			return

		self.logger.info("Loading %i transient(s) " % len(chunked_tran_ids))
		state = self.exec_params['state']

		# For '$latest' state, the latest compoundid of each transient must be determined
		if state == "$latest":

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
				# {'_id': '5de2480f28bfca0bd3baae890cb2d2ae', 'tranId': 'ZTF18aaayyuq'},
					# {'_id': '5fcdeda5e116989a69f6cbbde7234654', 'tranId': 'ZTF18aaabikt'},
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
			chunked_tran_ids, self.exec_params['channels'], states, 
			self.exec_params['docs'], self.exec_params['t2s'], 
			self.exec_params['feedback'], self.exec_params['verbose_feedback']
		)

		# For each task, create transientView and run task
		for t3_task in self.t3_job.get_tasks():

			self.logger.info(
				"Running task with t3Unit %s and runConfig %s" % (
					t3_task.get_config("t3Unit"), 
					t3_task.get_config("runConfig")
				)
			)

			# Get channel associated with this task
			task_chans = t3_task.get_config('select.channel(s)')

			# Build specific array of ampel TransientView instances where each transient 
			# is cut down according to the specified sub-selections parameters
			tran_views = []
			for tran_id, tran_data in al_tran_data.items():

				tran_view = tran_data.create_view(
					channel=task_chans if type(task_chans) is not list else None, 
					channels=task_chans if type(task_chans) is list else None, 
					t2_ids=t3_task.get_config('select.t2(s)')
				)
				
				if tran_view is not None:
					self.logger.debug(
						"TransientView created for %s and channel(s) %s" % 
						(tran_id, task_chans)
					)
					tran_views.append(tran_view)

			# get T3 unit instance (instantiate if first access)
			t3_unit = t3_task.get_t3_unit_instance(self.logger)

			# Run T3 instance
			self.logger.info("Adding TransientView instances to T3 unit %s" % t3_unit.__class__)
			t3_unit.add(tran_views)

		return len(chunked_tran_ids)
