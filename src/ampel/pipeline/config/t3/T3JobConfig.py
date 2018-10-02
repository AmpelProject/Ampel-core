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
from ampel.pipeline.config.GettableConfig import GettableConfig
from ampel.pipeline.config.t3.ScheduleEvaluator import ScheduleEvaluator
from ampel.pipeline.config.t3.T3TranConfig import T3TranConfig
from ampel.pipeline.config.t3.T3TaskConfig import T3TaskConfig

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
	transients: Union[None, T3TranConfig] = None
	tasks: Union[T3TaskConfig, List[T3TaskConfig]]


	@staticmethod
	def be_verbose():
		""" """
		T3JobConfig.logger = AmpelLogger.get_unique_logger()


	@staticmethod
	def logic_error_feedback():
		""" """
		AmpelLogger.get_unique_logger().error("T3JobConfig logic error")


	@validator('schedule')
	def schedule_must_not_contain_bad_things(cls, schedule):
		"""
		Safety check for "schedule" parameters 
		"""
		scheduler = module_schedule.Scheduler()
		evaluator = ScheduleEvaluator()
		for el in schedule if type(schedule) is str else [schedule]:
			evaluator(scheduler, schedule).do(nothing)
		return schedule


	@validator('tasks')
	def complicated_tasks_validation(cls, tasks, values, **kwargs):
		""" """

		# We allow for convenience to provide a task also as dict 
		# but let's wrap in into a list internally 
		if type(tasks) is not list:
			tasks = [tasks]

		# Rare: job does not require transients as input
		if values.get('transients', None) is None:
			# Make sure tasks do not require transients
			for task_config in tasks:
				if task_config.transients is None:
					raise ValueError(
						"T3 task logic error: field 'transients' cannot be " +
						"defined in task(s) if not defined in job"
					)
			return tasks

		# This information is required to validate tasks (see method doctring)
		joined_tasks_selections = T3JobConfig.merge_tasks_selections(tasks)

		# Check channel sub-selection of each task
		for task_config in tasks:
			T3JobConfig.validate_channel_sub_selection(
				values['transients'], task_config.transients, joined_tasks_selections
			)

		# Check channel sub-selection of all tasks combined
		T3JobConfig.check_correct_use_of_foreach(joined_tasks_selections)

		return tasks


	@staticmethod
	def merge_tasks_selections(task_configs):
		"""
		Merges together all "channels", "t2s" and "docs" values 
		from all tasks defined in job.
		This information is required to validate tasks.

		:returns: dict with key="channels"/"t2s"/"docs", value=<set of strings>
		"""
		joined_tasks_selections = {}

		# Build set of channels/t2s/docs for all tasks combined
		for task_config in task_configs if type(task_configs) is list else [task_configs]:

			task_tran_config = task_config.transients

			if not hasattr(task_tran_config, 'select'):
				continue

			for key in ('channels', 't2s', 'docs'):
				if hasattr(task_tran_config.select, key):
					if key not in joined_tasks_selections:
						joined_tasks_selections[key] = set()
					joined_tasks_selections[key].update(
						AmpelUtils.to_set(
							getattr(task_tran_config.select, key)
						)
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

		# Case 1, 6, 7
		if job_tran_config.select.channels is None:

			# Case 1, 7
			if task_tran_config.select.channels:

				# Case 7
				if task_tran_config.select.channels == "$forEach":
					pass
					
				# Case 1
				else:
					T3JobConfig.logic_error_feedback()
					raise ValueError(
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
			if task_tran_config.select.channels:

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
						T3JobConfig.logic_error_feedback()
						raise ValueError(
							"channels defined in task 'select' must be a sub-set "+
							"of channels defined in job config"
						)

					# case 3:
					if len(set(set_job_chans) - joined_tasks_selections['channels']) > 0:
						T3JobConfig.logic_error_feedback()
						raise ValueError(
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
	def check_correct_use_of_foreach(joined_tasks_selections):
		""" """
		# Case channels == $forEach
		if 'channels' in joined_tasks_selections and '$forEach' in joined_tasks_selections['channels']:

			# Either none or all tasks must make use of the $forEach operator
			if len(joined_tasks_selections['channels']) != 1:
				T3JobConfig.logic_error_feedback()
				raise ValueError(
					"Illegal task sub-channel selection: Either none task or all " +
					"tasks must make use of the $forEach operator"
				)

