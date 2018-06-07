#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/t3/T3Executor.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 26.02.2018
# Last Modified Date: 28.05.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from ampel.pipeline.logging.LoggingUtils import LoggingUtils
from ampel.flags.TransientFlags import TransientFlags
from ampel.pipeline.db.query.QueryMatchTransients import QueryMatchTransients
from ampel.pipeline.t3.TransientLoader import TransientLoader
from ampel.pipeline.db.query.QueryLatestCompound import QueryLatestCompound
from ampel.flags.AlDocTypes import AlDocTypes
import itertools


class T3Executor():
	"""
	"""

	@staticmethod
	def run_job(al_config, central_db, t3_job, logger):
		"""
		al_config: ampel config (dict)
		"""

		tran_col = central_db['main']


		# JOB WITHOUT INPUT
		# -----------------

		# T3 job not requiring any prior transient loading 
		if t3_job.get_config('input.select') is None:

			# Get task (transientless jobs can have only one task)
			t3_task = t3_job.get_task()

			# Instantiate T3 unit
			t3_unit = t3_task.get_t3_unit(logger)

			# Run T3 instance
			t3_unit.run(
				t3_task.get_run_config().get_parameters()
			)

			return


		# JOB WITH TRANSIENTS INPUT
		# -------------------------



		# 1) Get transient IDs
		######################

		# Build query for matching transients using criteria defined in job_config
		trans_match_query = QueryMatchTransients.match_transients(
			time_created = getattr(t3_job, "tran_sel_time_created", None),
			time_modified = getattr(t3_job, "tran_sel_time_created", None),
			channels = t3_job.get_config('input.select.channel(s)'),
			with_flags = t3_job.get_config('input.select.withFlag(s)'),
			without_flags = t3_job.get_config('input.select.withoutFlag(s)'),
		)

		logger.info("Executing search query: %s" % trans_match_query)

		# Execute 'find transients' query
		tran_ids_cursor = tran_col.find(
			trans_match_query, {'_id':0, 'tranId':1}
		).batch_size(100000)
		
		# Count results 
		c_count = tran_ids_cursor.count()
		if c_count == 0:
			logger.info("No transient matches the given criteria")
			return

		# Set chunk_size to 'number of transients found' if not defined
		chunk_size = t3_job.get_chunk()
		if chunk_size is None:
			chunk_size = c_count

		# 'State' to load is mandatory job option (when transients loading is requested)
		load_state = t3_job.tran_load_options("state") 

		# See if the current job is associated with more than one task or not
		multi_task_job = t3_job.get_tasks() is not None

		# Required to load single transients
		tl = TransientLoader(central_db, al_config, verbose, logger)



		# 2a) Single state (latest) loading
		###################################

		# For 'latest' state, the latest compoundid of each transient must be determined
		if load_state is not None and load_state == "latest":

			# Loop of chunk_size
			while True:

				# Get latest state ** for each channel **
				#########################################

				# Load ids (chunk_size number of ids)
				chunked_tran_ids = tuple(
					el['tranId'] for el in itertools.islice(tran_ids_cursor, chunk_size)
				)

				if len(chunked_tran_ids) == 0:
					logger.info("Breaking loop")
					break

				logger.info("Found %i transient(s) matching criteria" % len(chunked_tran_ids))
				logger.info("Retrieving latest state")

				# See for which ids the fast query cannot be used (save results in a set)
				slow_ids = set(
					el['tranId'] for el in tran_col.find(
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

				# List of dicts saving tranId, latest compoundID and channel id (if multi_task_job)
				latest_states = []

				# Channel/Channels must be provided if state is 'latest'
				for channel in t3_job.get_config('input.select.channel(s)'):

					# get latest state (fast mode) 
					# Output example:
					# [
					# {'_id': '5de2480f28bfca0bd3baae890cb2d2ae', 'tranId': 'ZTF18aaayyuq'},
 					# {'_id': '5fcdeda5e116989a69f6cbbde7234654', 'tranId': 'ZTF18aaabikt'},
					# ...
					# ]
					tmp_latest_states = [
						el for el in tran_col.aggregate(
							QueryLatestCompound.fast_query(
								slow_ids.symmetric_difference(chunked_tran_ids), 
								channel
							)
						).batch_size(chunk_size)
					]

					if multi_task_job:
						# Insert channel into returned dict
						# -> {'_id': '5de2480f2...', 'tranId': 'ZTF18aaayyuq', 'chan': HU_SN1},
						for el in tmp_latest_states:
							el['chan'] = channel

					# get latest state (general mode) for the remaining transients
					for tran_id in slow_ids:

						# get latest state for single transients using general query
						g_latest_state = next(
							tran_col.aggregate(
								QueryLatestCompound.general_query(
									tran_id, 
									project={
										'$project': {'tranId':1}
									}
								)
							).batch_size(chunk_size),
							None
						)

						# Robustness
						if g_latest_state is None:
							# TODO: add error flag to transient doc ?
							logger.error(
								"Could not retrieve latest state for transient %s" % 
								tran_id
							)

						if multi_task_job:
							# Insert channel into returned dict
							# -> {'_id': '5de2480f2...', 'tranId': 'ZTF18aaayyuq', 'chan': HU_SN1},
							for el in tmp_latest_states:
								el['chan'] = channel

					# Update latest_states with results from this loop 
					latest_states += tmp_latest_states


				# Load transient(s) with given state(s)
				#######################################

				logger.info("Loading transient(s)")

				# This array will contain the ampel.base.Transient instances
				al_trans = []

				# Build dict d_states using dict latest_states:
				# (latest state can be different from one channel to another)
				# {
				#	'ZTF18aaayyuq': '5de2480f28bfca0bd3baae890cb2d2ae',
				#	'ZTF18azzzzzz': [   
				# 		'51111111112222222223333334444444',
				# 		'88773246873246782364732642384444',
				#		...
				# 	]
				# }
				d_states = {}
				for el in latest_states:
					if el['tranId'] in d_states: 
						if type(d_states[el['tranId']]) is list:
							d_states[el['tranId']].append(el['_id'])
						else:
							d_states[el['tranId']] = [
								d_states[el['tranId']], el['_id']
							]
					else:
						d_states[el['tranId']] = el['_id']
				
				# Load ampel transient objects
				for tran_id in d_states:
					
					# Populate internal array al_trans ...
					al_trans.append(

						# ... with new instance of ampel.base.Transient
						tl.load_new(
							tran_id, 
							state=d_states[tran_id], 
							t2_ids=t3_job.get_config('input.load.t2(s)')
						)
					)

				# A single task is associated with this job
				if not multi_task_job:

					# get T3 unit instance (instantiate if first access)
					t3_unit = t3_task.get_t3_unit(logger)

					logger.info("Running instance of %s" % t3_unit.__class__)

					# Run T3 instance
					t3_unit.run(
						t3_task.get_run_config().get_parameters(),  al_trans
					)

				else:

					# Fork transients for each task and run it
					##########################################

					# Run tasks
					for t3_task in t3_job.get_tasks():

						logger.info(
							"Running task with t3Unit %s and runConfig %s" %
							(t3_task.get_parameter("t3Unit"), t3_task.get_parameter("runConfig"))
						)
	
						# Get channel associated with this task
						task_chan = t3_task.get_selection('channel')

						# New list for forked transients (see below)
						al_task_trans = []

						print ("latest_states")
						print (latest_states)

						# Build specific array of ampel transient instances where each transient 
						# is cut down according to the specified sub-selections parameters
						for transient in al_trans:

							# Get 'latest state' asssociated with this transient and channel
							for el in latest_states:
								if transient.tran_id == el['tranId'] and task_chan == el['chan']:
									tran_latest_state = el['_id']
						
							# Transient is not associated with current channel
							if tran_latest_state is None:
								# TODO: add debug info ?
								# "Latest state could not be found for transient %s and channel %s" %
								# (transient.tran_id, task_chan)
								continue

							# fork transient
							ftran = TransientForker.fork(
								transient, 
								doc_type = t3_task.get_selection('docType'), 
								state = tran_latest_state, 
								channel = task_chan, 
								t2_unit_ids = t3_task.get_selection('t2Id'),
								pps_must_flags = t3_task.get_pps_must_flags()
							)

							# inform transient that the only loaded state is the latest
							ftran.set_latest_compound_id(tran_latest_state)

							# Populate list of transient 
							al_task_trans.append(ftran)
							
							# Reset this just in case
							tran_latest_state = None

	
						# get T3 unit instance (instantiate if first access)
						t3_unit = t3_task.get_t3_unit(logger)
	
						logger.info("Running instance of %s" % t3_unit.__class__)

						# Run T3 instance
						t3_unit.run(
							t3_task.get_run_config().get_parameters(), transients=al_task_trans
						)


		# 2b) Load full transients (all state)
		######################################

		else:

			raise NotImplementedError("Not implemented yet")
			#tran_ids = [<load transient> el['tranId'] for el in tran_ids_cursor]
