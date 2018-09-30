#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/t3/T3TaskConfig.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 05.07.2018
# Last Modified Date: 17.07.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import pkg_resources
from voluptuous import Schema, Any, Required, Optional, ALLOW_EXTRA

from ampel.pipeline.logging.AmpelLogger import AmpelLogger
from ampel.pipeline.common.AmpelUtils import AmpelUtils
from ampel.pipeline.config.AmpelConfig import AmpelConfig
from ampel.base.abstract.AbsT3Unit import AbsT3Unit
from ampel.pipeline.t3.T3Task import T3Task

class T3TaskConfig:
	"""
	"""

	# Static schema for t3 tasks
	t3_task_schema = Schema(
		{
			Required('name'): str,
			Required('t3Unit'): str,
			Required('runConfig', default=None): Any(None,str),
			'select': {
				'channel(s)': Any(str, [str]),
				'state': Any('$all', '$latest'),
				't2(s)': Any(str, [str])
			},
			Optional('verbose', default=False): bool
		},
		extra=ALLOW_EXTRA
	)

	# Static schema for t3 units
	t3_unit_schema = Schema(
		{
			Required('classFullPath'): str,
			Optional('baseConfig', default=None): Any(None, dict),
			Optional('verbose', default=False): bool
		},
		extra=ALLOW_EXTRA
	)

	# Static dict instance referencing already loaded t3 *classes* (not instances)
	# in order to avoid multiple reloading of t3 classes shared among several
	# different tasks (also accross multiple jobs)
	t3_classes = {}


	@classmethod
	def get_t3_unit(cls, t3_unit_name, logger):
		"""
		t3_unit_name: string
		logger: logger from python module 'logging'
		Retrieve and return T3 unit class registered at entry point ampel.pipeline.t3.units
		"""
		if t3_unit_name in cls.t3_classes:
			return cls.t3_classes[t3_unit_name]
		
		logger.info("Loading T3 unit config: %s" % t3_unit_name)
		try:
			resource = next(pkg_resources.iter_entry_points('ampel.pipeline.t3.units', t3_unit_name))
		except StopIteration:
			raise ValueError(
				"Unknown T3 unit: %s" % t3_unit_name
			)
		klass = resource.resolve()
		if not issubclass(klass, AbsT3Unit):
			raise TypeError(
				"T3 unit {} from {} is not a subclass of AbsT3Unit".format(klass.__name__, resource.dist)
			)
		cls.t3_classes[t3_unit_name] = klass
		return klass


	@staticmethod
	def get_run_config_doc(task_doc, logger):
		"""
		task_doc: dict instance
		logger: logger from python module 'logging'
		Retrieve and return T3 unit run config (dict instance)
		"""

		# Load run_config 
		if task_doc.get('runConfig') is None:
			return None
		
		run_config = 't3RunConfig.%s_%s' % (task_doc['t3Unit'], task_doc['runConfig'])
		logger.info("Loading T3 run config: %s" % run_config)

		t3_run_config_doc = AmpelConfig.get_config(run_config)
		if t3_run_config_doc is None:
			raise ValueError(
				"Run config %s_%s not found, please check your config" %
				(task_doc['t3Unit'], task_doc['runConfig'])
			)

		return t3_run_config_doc


	@classmethod
	def load(cls, job_name, task_name, all_tasks_sels=None, logger=None):
		"""
		job_name: name of the job parent of this task
		task_name: name of this task
		all_tasks_sels: used for internal optimizations
		logger: logger instance from python module 'logging'

		returns an instance of ampel.pipeline.t3.T3TaskConfig
		"""

		t3_job_doc = AmpelConfig.get_config('t3Jobs').get(job_name)

		if t3_job_doc is None:
			raise ValueError("Job %s not found" % job_name)
		
		return cls.from_doc(t3_job_doc, task_name, all_tasks_sels, logger)


	@classmethod
	def from_doc(cls, t3_job_doc, task_name, all_tasks_sels=None, logger=None):
		"""
		"""

		task_doc = None
		if all_tasks_sels is None:

			all_tasks_sels = {}

			# Get t3_task_doc with provided name and build 
			# set of channel(s)/t2(s)/doc(s) for all tasks combined
			for doc in t3_job_doc['task(s)']:

				if doc['name'] == task_name:

					# Get, check and set defaults of t3 task doc. Don't do it if type is 
					# MappingProxyType since it means config was already validated
					task_doc = doc if AmpelConfig.is_frozen() else cls.t3_task_schema(doc)
				
				if 'select' not in doc:
					continue

				for key in ('channel(s)', 't2(s)', 'doc(s)'):
					if doc['select'].get(key) is not None:
						if key not in all_tasks_sels:
							all_tasks_sels[key] = set()
						all_tasks_sels[key].update(
							AmpelUtils.to_set(doc['select'][key])
						)
		else:

			task_doc = next(filter(lambda x: x['name'] == task_name, t3_job_doc['task(s)']))
			if not AmpelConfig.is_frozen(): # validate task doc if not done previously
				task_doc = cls.t3_task_schema(task_doc)

		if task_doc is None:
			raise ValueError("Task %s not found" % task_name)

		# Internal variables
		name = task_doc['name']
		t3_unit_name = task_doc['t3Unit']

		# Setup logger
		logger = AmpelLogger.get_logger() if logger is None else logger
		logger.info("Checking T3 task '%s'" % task_doc['name'])


		# Robustness
		############

		# Save transient sub-selection criteria if provided
		if 'select' in task_doc:

			""" 
			channels sub-selection validity 
			-------------------------------

			In the following: 
			* 'a' and 'b' are channel names
			* Task channels means attribute 'channels' defined in the task config
			* Job channels means attribute 'channels' defined in the job config

			=x=>   means forbidden (arrow with cross)
			===>   means ok
			
			#######				#########
			# JOB #				# TASKS #		# Comment #
			#######				#########

			1)  None	 =X=> 	{a, b}			Channels 'a' and 'b' must be defined 
												in Job for query efficiency

			2)  {a, b}	 =X=> 	{a, b, c}		Task channels must be a sub-set of job channels

			3)  {a, b}	 =X=> 	{a}				Job channels must be equal to set 
												of combined tasks channels (see 4)

			- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

			4)  {a, b}	 ===> 	Task 1: {a} 	OK
								Task 2: {b}

			5)  {a, b}	 ===> 	None			Transients provided to T3 units (defined in tasks)
												will be so-called 'multi-channel' transients

			6)  None	 ===> 	None			Transients provided to T3 units (defined in tasks)
												will be so-called 'multi-channel' transients

			7)  None	 ===> 	"$forEach"		If "$forEach" is provided as channel name, the corresponding
												task(s) will be executed separately for each channel 
												returned by the criteria defined in the job selection.
												
			8)  {a, b}	 ===> 	"$forEach"		same as 7)
			""" 

			# Case 1, 6, 7
			if AmpelUtils.get_by_path(t3_job_doc, "input.select.channel(s)") is None:

				# Case 1, 7
				if 'select' in task_doc and 'channels' in task_doc['select']:

					# Case 7
					if task_doc['select']['channel(s)'] == "$forEach":
						pass
						
					# Case 1
					else:
						cls._raise_ValueError(logger, task_doc,
							"Channels %s must be defined in parent job config" % 
							task_doc['select']['channel(s)']
						)

				# Case 6
				else:
					logger.info(
						"Mixed-channels transients will be provided to %s of task %s" %
						(t3_unit_name, task_doc['name'])
					)

			# Case 2, 3, 4, 5, 8
			else:

				# Case 2, 3, 4, 8
				if 'select' in task_doc and 'channel(s)' in task_doc['select']:

					# Case 8
					if task_doc['select']['channel(s)'] == "$forEach":
						pass

					# Case 2, 3, 4
					else:

						set_task_chans = AmpelUtils.to_set(
							task_doc['select']['channel(s)']
						)

						set_job_chans = AmpelUtils.to_set(
							AmpelUtils.get_by_path(t3_job_doc, "input.select.channel(s)")
						)

						# case 2
						if len(set_task_chans - set_job_chans) > 0:
							cls._raise_ValueError(logger, task_doc,
								"channel(s) defined in task 'select' must be a sub-set "+
								"of channel(s) defined in job config"
							)

						# case 3:
						if len(set(set_job_chans) - all_tasks_sels['channel(s)']) > 0:
							cls._raise_ValueError(logger, task_doc,
								"Set of job channel(s) must equal set of combined tasks channel(s)"
							)

						# case 4
						logger.info("Tasks sub-channel selection is valid")

				# Case 5
				else:
					logger.info(
						"Mixed-channels transients will be provided to %s of task %s" %
						(t3_unit_name, task_doc['name'])
					)
					

			# t2s sub selection robustness
			cls._subset_check(task_doc, t3_job_doc, "t2(s)", logger)

			# docs sub selection robustness
			cls._subset_check(task_doc, t3_job_doc, "doc(s)", logger)

			# withFlags sub selection robustness
			cls._subset_check(task_doc, t3_job_doc, "withFlag(s)", logger)

			# withoutFlags sub selection robustness
			cls._subset_check(task_doc, t3_job_doc, "withoutFlag(s)", logger)

			# Further robustness check
			if AmpelUtils.get_by_path(task_doc, "select.t2(s)") is not None:

				docs_subsel = AmpelUtils.get_by_path(task_doc, "select.doc(s)")
				if docs_subsel is None:

					t2s_job_sel = AmpelUtils.get_by_path(t3_job_doc, "input.load.doc(s)")
					if t2s_job_sel is not None and "T2RECORD" not in t2s_job_sel:

						cls._raise_ValueError(logger, task_doc, 
							"T2RECORD must be included in job input->load->doc(s) when "+
							"Task select->t2(s) filtering is configured"
						)
				else:
					if "T2RECORD" not in docs_subsel:

						cls._raise_ValueError(logger, task_doc, 
							"T2RECORD must be included in select->doc(s) when "+
							"select->t2(s) filtering is configured"
						)


			# Check validity of state sub-selection
			if 'state' in task_doc['select']:

				# Allowed:   main:'$all' -> sub:'$all' 
				# Allowed:   main:'$latest' -> sub:'$latest' 
				# Allowed:   main:'$all' -> sub:'$latest' 
				# Denied:    main:'$latest' -> sub:'$all' 
				requested_state_by_job = AmpelUtils.get_by_path(t3_job_doc, 'input.load.state')
				if requested_state_by_job != AmpelUtils.get_by_path(task_doc, 'select.state'):
					if requested_state_by_job == '$latest':
						cls._raise_ValueError(logger, task_doc,
							"invalid state sub-selection criteria: main:'$latest' -> sub:'$all"
						)


		# Create TaskConfig
		###################

		return T3TaskConfig(
			task_doc, 
			cls.get_t3_unit(t3_unit_name, logger),
			cls.get_run_config_doc(task_doc, logger), 
			logger
		)


	@classmethod
	def _subset_check(cls, task_doc, t3_job_doc, key, logger):
		"""
		"""

		# Check validity of t2s/docs sub-selection
		# No top level t2s/docs selection means *all* t2s/docs
		# Allowed:             		case 1)		main:'nosel' -> sub:'sel' 
		# Allowed:             		case 2)		main:'nosel' -> sub:'nosel' 
		# Allowed if subset:   		case 3)		main:'sel' -> sub:'other sel' 
		# Forbidden if no subset:	case 4)		main:'sel' -> sub:'other sel' 

		task_select_value = AmpelUtils.get_by_path(task_doc, 'select.%s' % key)
		job_load_value = AmpelUtils.get_by_path(t3_job_doc, 'input.load.%s' % key)


		# Case 1 and 2
		if job_load_value is None:

			# Case 1
			if task_select_value is not None:
				logger.info(
					"Specific %s selection requested: %s" %
					(key, task_select_value)
				)
			# Case 2
			else:
				pass

		# Case 3, 4
		else:

			if task_select_value is not None:

				set_job = AmpelUtils.to_set(job_load_value)
				set_task = AmpelUtils.to_set(task_select_value)

				# Case 3
				if len(set_task - set_job) == 0:
					logger.info(
						"Specific %s sub-selection requested: %s" % 
						(key, task_select_value)
					)
				# Case 4
				else:
					T3TaskConfig._raise_ValueError(logger, task_doc,
						"Invalid Task %s sub-selection (no subset of Job %s selection)" %
						(key, key)
					)


	@staticmethod	
	def _raise_ValueError(logger, task_doc, msg):
		"""
		"""
		logger.error("Invalid %s T3 task config" % task_doc['name'])
		raise ValueError(msg)


	def __init__(self, task_doc, t3_unit, t3_unit_run_config_doc, logger):
		"""
		task_doc: dict instance
		t3_unit: type, subclass of AbsT3Unit
		t3_unit_run_config_doc: dict instance containing t3 run config info
		logger: logger instance from python module 'logging' 
		"""

		logger.info("Loading Task: %s" % task_doc.get("name"))

		self.task_doc = task_doc
		self.t3_unit_class = t3_unit
		self.t3_resources = {
			k: AmpelConfig.get_config('resources.{}'.format(k)) 
			for k in getattr(t3_unit, 'resources', [])
		}
		self.t3_unit_run_config = t3_unit_run_config_doc

		self.channels = AmpelUtils.get_by_path(task_doc, 'select.channel(s)')
		self.t2_ids = AmpelUtils.get_by_path(task_doc, 'select.t2(s)')
		self.log_header = "Task %s (t3Unit: %s, runConfig: %s)" % (
			task_doc.get("name"), task_doc.get("t3Unit"), task_doc.get("runConfig"),
		)

		logger.info("T3 Unit: %s" % t3_unit)
		logger.info("Run config: %s" % self.t3_unit_run_config)
		logger.info("Channels: %s" % str(self.channels))


	def get(self, parameter):
		"""
		parameter: string
		"""
		return AmpelUtils.get_by_path(self.task_doc, parameter)


	def create_task(self, logger, channels=None, global_info=None):
		"""
		logger: logger instance from python module 'logging' 
		channels: optional channels name
		"""

		if global_info is not None:
			global_info = dict(global_info)
			global_info['taskName'] = self.task_doc.get("name")
			global_info['channel(s)'] = self.channels if channels is None else channels

		return T3Task(
			self, 
			self.channels if channels is None else channels,
			logger,
			global_info
		)
