#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/config/t3/T3JobConfig.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 29.09.2018
# Last Modified Date: 01.10.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import schedule as module_schedule
from pydantic import BaseModel, validator
from typing import Union, List
from ampel.pipeline.logging.AmpelLogger import AmpelLogger
from ampel.pipeline.common.AmpelUtils import AmpelUtils
from ampel.pipeline.common.docstringutils import gendocstring
from ampel.pipeline.config.AmpelBaseModel import AmpelBaseModel
from ampel.pipeline.config.AmpelConfig import AmpelConfig
from ampel.pipeline.config.ReadOnlyDict import ReadOnlyDict
from ampel.pipeline.config.GettableConfig import GettableConfig
from ampel.pipeline.config.t3.ScheduleEvaluator import ScheduleEvaluator
from ampel.pipeline.config.t3.T3TaskConfig import T3TaskConfig
from ampel.pipeline.config.t3.TranConfig import TranConfig
from ampel.pipeline.config.t3.TranSelectConfig import TranSelectConfig
from ampel.pipeline.config.t3.TranContentConfig import TranContentConfig

def nothing():
	pass

@gendocstring
class T3JobConfig(BaseModel, GettableConfig):
	"""
	Possible 'schedule' values (https://schedule.readthedocs.io/en/stable/):
	"every(10).minutes"
	"every().hour"
	"every().day.at("10:30")"
	"every().monday"
	"every().wednesday.at("13:15")"

	Note: Use `T3JobConfig.be_verbose()` if you want feedback
	"""
	job: str
	active: bool = True
	globalInfo: bool = False
	schedule: Union[str, List[str]]
	transients: Union[None, TranConfig]
	tasks: Union[T3TaskConfig, List[T3TaskConfig]]


	@staticmethod
	def be_verbose():
		""" """
		T3JobConfig.logger = AmpelLogger.get_unique_logger()


	@validator('transients', 'tasks', pre=True, whole=True)
	def unfreeze_if_frozen(cls, arg):
		if AmpelConfig.has_nested_type(arg, ReadOnlyDict):
			return AmpelConfig.recursive_unfreeze(arg)
		return arg


	@validator('schedule', 'tasks', pre=True, whole=True)
	def cast_to_list(cls, v):
		if type(v) is not list:
			return [v]
		return v


	@validator('tasks', pre=True, whole=True)
	def do_before_validation(cls, tasks, values, **kwargs):
		""" 
		tasks is a dict (pre is True) but values['transients'] is a TranConfig obj.
		Here we add some info extracted from the job into the tasks
		so that the task validators do not complain about possible null values.
		"""

		# Happens also when exceptions are raised in linked objects
		if values.get('transients', None) is None:
			return tasks

		# forward 'transient' attributes from job to tasks
		# -> makes task validation easier
		# -> usefull if one wants to run this task in a stand-alone way
		for task_config in tasks: # task_config is a dict 

			# Copy entire 'transients' value if missing
			if task_config.get('transients') is None:
				task_config['transients'] = values['transients']
				continue

			import copy
			values['transients'] = copy.deepcopy(values['transients'])

			# Copy state value if missing
			if 'state' not in task_config['transients']:
				task_config['transients']['state'] = values['transients'].get('state')

			# Either copy entire 'transients.select' and 'transients.content' values if missing
			# or the sub-key values (t2SubSelection, docs, channels, withFlags ...) that were not set
			for el in {'select': TranSelectConfig, 'content': TranContentConfig}.items():

				cont_or_sel_str = el[0]
				job_cont_or_sel_conf = values['transients'].get(cont_or_sel_str)
				task_cont_select_conf = task_config['transients'].get(cont_or_sel_str)

				# Inherit load/select value from job 
				if task_cont_select_conf is None:
					task_config['transients'][cont_or_sel_str] = job_cont_or_sel_conf
				else:

					# el.value is either T3TranContentConfig or T3TranSelectConfig
					for field in el[1].__fields__.keys():

						# Don't override task specific values
						if task_cont_select_conf.get(field) is not None:

							# But perform some validation of the following docs while we are here
							if field in ["docs", "t2SubSelection", "channels", "withFlags", "withoutFlags"]:

								# Allowed: job: no selection -> task: selection
								if job_cont_or_sel_conf.get(field) is None:
									continue

								# build sets to easily check subselections 
								set_job = AmpelUtils.to_set(job_cont_or_sel_conf.get(field))
								set_task = AmpelUtils.to_set(task_cont_select_conf.get(field))

								# Allowed (subset): job: selection -> task: sub-selection
								if len(set_task - set_job) == 0:
									continue
								else:
									# Forbidden: no-subset
									AmpelUtils.print_and_raise(
										"T3JobConfig logic error",
										"Invalid Task %s sub-selection (no subset of Job %s selection)" %
										(field, field)
									)
								pass

							continue

						task_cont_select_conf[field] = job_cont_or_sel_conf.get(field)

		return tasks
	

	@validator('tasks', whole=True)
	def validate_tasks(cls, tasks, values, **kwargs):
		""" """

		# Rare: job does not require transients as input
		if values.get('transients', None) is None:
			# Make sure tasks do not require transients
			for task_config in tasks:
				if task_config.transients is None:
					AmpelUtils.print_and_raise(
						"T3JobConfig logic error",
						"T3 task logic error: field 'transients' cannot be " +
						"defined in task(s) if not defined in job"
					)
			if hasattr(T3JobConfig, 'logger'):
				T3JobConfig.logger.info('Job not requiring TransientViews as input')
			return tasks

		# This information is required to validate tasks (see method doctring)
		joined_tasks_selections = T3JobConfig.get_merged_tasks_selections(tasks)
		job_state = values["transients"].state

		# Check sub-selection of each task
		for task_config in tasks:

			# Channel
			T3JobConfig.validate_channel_sub_selection(
				values['transients'], 
				task_config.transients, 
				joined_tasks_selections
			)

			# State
			T3JobConfig.check_task_state(
				job_state, task_config.transients.state
			)


		# Check $forEach channel sub-selection of all tasks combined
		if '$forEach' in joined_tasks_selections.get('channels', []):

			# Either none or all tasks must make use of the $forEach operator
			if len(joined_tasks_selections['channels']) != 1:
				AmpelUtils.print_and_raise(
					"T3JobConfig logic error",
					"Illegal task sub-channel selection: Either none task or all " +
					"tasks must make use of the $forEach operator"
				)

		return tasks


	@validator('schedule', whole=True)
	def schedule_must_not_contain_bad_things(cls, schedule):
		"""
		Safety check for "schedule" parameters 
		"""
		evaluator = ScheduleEvaluator()
		for el in schedule:
			try:
				evaluator(module_schedule.Scheduler(), el).do(nothing)
			except Exception as e:
				print("#"*len(str(e)))
				print("ScheduleEvaluator exception:")
				print(str(e))
				print("#"*len(str(e)))
				raise

		return schedule


	@staticmethod
	def get_merged_tasks_selections(task_configs):
		"""
		Merges together all "channels", "t2SubSelection" and "docs" values 
		from all tasks defined in job.
		This information is required to validate tasks.

		:returns: dict with key="channels"/"t2SubSelection"/"docs", value=<set of strings>
		"""
		joined_tasks_selections = {}

		# Build set of channels/t2SubSelection/docs for all tasks combined
		for task_config in task_configs if type(task_configs) is list else [task_configs]:

			tran_config = task_config.transients

			if tran_config.get('select.channels'):
				if 'channels' not in joined_tasks_selections:
					joined_tasks_selections['channels'] = set()
				joined_tasks_selections['channels'].update(
					AmpelUtils.to_set(tran_config.select.channels)
				)

			for key in ('t2SubSelection', 'docs'):
				val = tran_config.content.get(key)
				if val:
					if key not in joined_tasks_selections:
						joined_tasks_selections[key] = set()
					joined_tasks_selections[key].update(
						AmpelUtils.to_set(val)
					)

		return joined_tasks_selections


	@staticmethod
	def validate_channel_sub_selection(
		job_tran_config, task_tran_config, joined_tasks_selections
	):

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
											tasks will be executed separately for each channel 
											returned by the criteria defined in the job selection.
											
		8)  {a, b}	 ===> 	"$forEach"		same as 7)
		""" 

		if task_tran_config is None:
			return

		# Case 1, 6, 7
		if job_tran_config.select.channels is None:

			# Case 1, 7
			if task_tran_config.select.channels:

				# Case 7
				if task_tran_config.select.channels == "$forEach":
					pass
					
				# Case 1
				else:
					AmpelUtils.print_and_raise(
						"T3JobConfig logic error",
						"Channels %s must be defined in parent job config" % 
						task_tran_config.select.channels
					)

			# Case 6
			else:
				if hasattr(T3JobConfig, 'logger'):
					T3JobConfig.logger.info(
						"Mixed-channels transients will be provided to task %s" %
						task_tran_config.task
					)

		# Case 2, 3, 4, 5, 8
		else:

			# Case 2, 3, 4, 8
			if task_tran_config.get('select.channels'):

				# Case 8
				if task_tran_config.select.channels == "$forEach":
					pass

				# Case 2, 3, 4
				else:

					set_task_chans = AmpelUtils.to_set(
						task_tran_config.select.channels
					)

					set_job_chans = AmpelUtils.to_set(
						job_tran_config.select.channels
					)

					# case 2
					if len(set_task_chans - set_job_chans) > 0:
						AmpelUtils.print_and_raise(
							"T3JobConfig logic error",
							"channels defined in task 'select' must be a sub-set "+
							"of channels defined in job config"
						)

					# case 3:
					if len(set(set_job_chans) - joined_tasks_selections['channels']) > 0:
						AmpelUtils.print_and_raise(
							"T3JobConfig logic error",
							"Set of job channels must equal set of combined tasks channels"
						)

					# case 4
					if hasattr(T3JobConfig, 'logger'):
						T3JobConfig.logger.info("Tasks sub-channel selection is valid")

			# Case 5
			else:
				if hasattr(T3JobConfig, 'logger'):
					T3JobConfig.logger.info(
						"Mixed-channels transients will be provided to task %s" %
						task_tran_config.task
					)
				

	@staticmethod
	def check_task_state(job_state, task_state):
		"""
		Allowed:   main:'$all' -> sub:'$all' 
		Allowed:   main:'$latest' -> sub:'$latest' 
		Allowed:   main:'$all' -> sub:'$latest' 
		Denied:    main:'$latest' -> sub:'$all' 
		"""
		if job_state != task_state:
			if job_state == '$latest':
				AmpelUtils.print_and_raise(
					"T3JobConfig error",
					"invalid state sub-selection criteria: main:'$latest' -> sub:'$all"
				)
