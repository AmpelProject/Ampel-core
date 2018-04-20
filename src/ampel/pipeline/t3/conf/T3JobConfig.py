#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/t3/conf/T3JobConfig.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 06.03.2018
# Last Modified Date: 11.03.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from ampel.pipeline.logging.LoggingUtils import LoggingUtils
from ampel.pipeline.t3.conf.T3TaskConfig import T3TaskConfig
from ampel.flags.TransientFlags import TransientFlags
from ampel.flags.FlagUtils import FlagUtils
from datetime import timedelta


class T3JobConfig:
	"""
	"""

	# static DB collection name
	_t3_jobs_colname = "t3_jobs"


	def __init__(self, config_db, db_doc=None, job_id=None, logger=None):
		"""
		Provide either db_doc or job_id:
		This class is mainly made of config validity tests.
		Though a bit of DB query here and class instanciation there occurs occasionally.
		NOTE: channel existence is not checked on purpose (as in the 'channels' mongodb collection) 
		"""

		# Robustness check
		if db_doc is None and job_id is None:
			raise ValueError("Please provide either db_doc or job_id")

		if logger is None:
			logger = LoggingUtils.get_logger()

		# Load DB document if not provided (parameter db_doc)
		if db_doc is None:

			# Lookup job entry in db
			cursor = config_db[T3JobConfig._t3_jobs_colname].find(
				{'_id': job_id}
			)

			# Robustness check
			if cursor.count() == 0:
				raise ValueError("Job %s not found" % job_id)

			# Retrieve job entry
			db_doc = next(cursor)
			self.id = db_doc['_id']

		else:
			self.id = job_id

		logger.info("Loading job %s" % self.id)
		self.job_doc = db_doc

		# Dict key 'schedule' must be defined
		if not "schedule" in db_doc:
			self._raise_ValueError(logger, "dict key 'schedule' missing")

		# Either 'task' or 'tasks' dict keys must be defined
		if not "task" in db_doc and not "tasks" in db_doc:
			self._raise_ValueError(logger, "dict key 'task(s)' missing")

		# If dict key transients is provided (meaning we have a job requiring 
		# loaded ampel.base.Transient instances to work with)
		if 'transients' in db_doc:

			# ... then subkeys 'load' and 'select' must be provided as well
			if not "select" in db_doc['transients'] and not "load" in db_doc['transients']:
				self._raise_ValueError(
					logger, 
					"values must be set for dict keys 'select' and 'load' " +
					"when dict key 'transients' is defined"
				)

			self.tran_sel = db_doc['transients']['select']
			self.tran_load = db_doc['transients']['load']

			# Select transient based on their creation date
			if "created" in self.tran_sel:

				self.tran_sel_time_created = {
					"delta": None, "from": None, "until": None
				}

				if "timedelta" in self.tran_sel["created"]:
					self.tran_sel_time_created["delta"] = timedelta(**self.tran_sel['created']['timedelta'])

				# TODO: implement from and until
				#if "from" in self.tran_sel["created"]:
				#	self.tran_sel_time_created["from"] = datetime.strptime('Jun 1 2005  1:33PM', '%b %d %Y %I:%M%p')


			# Select transient based on their modification date
			if "created" in self.tran_sel:

				self.tran_sel_time_created = {
					"delta": None, "from": None, "until": None
				}

				if "timedelta" in self.tran_sel["created"]:
					self.tran_sel_time_created["delta"] = timedelta(**self.tran_sel['created']['timedelta'])

				# TODO: implement from and until
				#if "from" in self.tran_sel["created"]:
			
			if "modified" in self.tran_sel:

				self.tran_sel_time_modified = {
					"delta": None, "from": None, "until": None
				}

				if "timedelta" in self.tran_sel["modified"]:
					self.tran_sel_time_modified["delta"] = timedelta(**self.tran_sel['modified']['timedelta'])


			# Transient state must be provided
			if not 'state' in self.tran_load:
				self._raise_ValueError(logger, "transient state must be specified")

			# And its value must be either 'all' or 'latest'
			if not self.tran_load['state'] in ['all', 'latest']:
				self._raise_ValueError(
					logger, 
					"transient state must be either 'all' or 'latest'"
				)

			# Optional 'chunk' parameter specifies how many transients at max 
			# a t3 unit will process at once
			if "chunk" in db_doc['transients']:
				if not type(db_doc['transients']['chunk']) is int:
					self._raise_ValueError(logger, "'chunk' parameter value type must be int")
				self.tran_chunk = db_doc['transients']['chunk']

			# Check that proper values were provided, if provided
			T3JobConfig._check_type_list(self.tran_sel, 'channels', str)
			T3JobConfig._check_type_list(self.tran_load, 't2Ids', str)
			T3JobConfig._check_type_list(self.tran_load, 'alDocTypes', int)
			T3JobConfig._check_type_string(self.tran_sel, 'channel')
			T3JobConfig._check_type_string(self.tran_load, 't2Id')

			# Convert to enum flags
			if 'withFlags' in self.tran_sel: 
				FlagUtils.list_flags_to_enum_flags(
					self.tran_sel['withFlags'],
					TransientFlags
				)

			# Convert to enum flags
			if 'withoutFlags' in self.tran_sel: 
				FlagUtils.list_flags_to_enum_flags(
					self.tran_sel['withoutFlags'],
					TransientFlags
				)

			
		# How should AMPEL react on error
		if 'onError' in db_doc:
			self.on_error = db_doc['onError']

		self.t3_tasks = []

		# Load and check task
		if 'task' in db_doc:
			self.t3_tasks.append(
				T3TaskConfig(
					config_db, db_doc['task'], 
					getattr(self, "tran_sel", None),
					getattr(self, "tran_load", None),
					logger
				)
			)

		# Load and check tasks
		elif 'tasks' in db_doc:

			# Make sure 'tasks' is a list of dict instances
			T3JobConfig._check_type_list(db_doc, 'tasks', dict)

			for t3_task_doc in db_doc['tasks']:
				self.t3_tasks.append(
					T3TaskConfig(
						config_db, t3_task_doc, 
						getattr(self, "tran_sel", None),
						getattr(self, "tran_load", None),
						logger
					)
				)


	def _raise_ValueError(self, logger, msg):
		""" """
		logger.error("Failing job doc: %s" % self.job_doc)
		raise ValueError("Invalid T3 job config: "+msg)


	def get_tasks(self):
		""" 
		Returns the loaded tasks associated with this job
		"""
		return self.t3_tasks


	def get_task(self):
		""" 
		Returns the unique task associated with this job or raise error
		if multiple tasks were loaded
		"""
		if len(self.t3_tasks) != 1:
			raise ValueError("Multiple tasks available")
		return self.t3_tasks[0]


	def get_chunk(self):
		""" """
		return getattr(self, "tran_chunk", None)


	def tran_sel_options(self, option=None):
		""" 
		Returns the transient selection criteria associated with this job
		"""
	
		if not hasattr(self, "tran_sel"):
			return None

		if option in self.tran_sel:
			return self.tran_sel[option] if option in self.tran_sel else None

		return self.tran_sel


	def tran_load_options(self, option=None):
		""" 
		Returns the transient loading options associated with this job
		"""
	
		if not hasattr(self, "tran_load"):
			return None

		if not option is None:
			return self.tran_load[option] if option in self.tran_load else None

		return self.tran_load


	def load_options_t2Ids(self):

		if not hasattr(self, "tran_load"):
			return None

		if "t2Id" in self.tran_load:
			return [self.tran_load['t2Id']]

		if "t2Ids" in self.tran_load:
			return self.tran_load['t2Ids']


	def sel_options_channel(self):

		if not hasattr(self, "tran_sel"):
			return None

		if "channel" in self.tran_sel:
			return [self.tran_sel['channel']]

		if "channels" in self.tran_sel:
			return self.tran_sel['channels']


	@staticmethod
	def _check_type_string(db_doc, key):
		""" 
		Internal robustness check function for job config entries
		"""
		if not key in db_doc:
			return

		if not type(db_doc[key]) is str:
			raise ValueError(
				"Invalid T3 job config: '%s' parameter value type must be str" % key
			)


	@staticmethod
	def _check_type_list(db_doc, key, should_type):
		""" 
		Internal robustness check function for job config entries
		"""
		if not key in db_doc:
			return

		if not type(db_doc[key]) is list:
			raise ValueError(
				"Invalid T3 job config: '%s' parameter value type must be list" % key
			)

		for value in db_doc[key]:
			if not type(value) is should_type:
				raise ValueError(
					"Invalid T3 job config: '%s' parameter value type must be %s" % 
					(key, should_type)
				)
