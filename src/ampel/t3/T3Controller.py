#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/t3/T3Controller.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 26.02.2018
# Last Modified Date: 13.08.2019
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import schedule, time 
from multiprocessing import Process
from ampel.t3.T3Job import T3Job
from ampel.t3.T3Task import T3Task
from ampel.config.t3.T3JobConfig import T3JobConfig
from ampel.config.t3.T3TaskConfig import T3TaskConfig
from ampel.common.Schedulable import Schedulable
from ampel.config.t3.ScheduleEvaluator import ScheduleEvaluator
from ampel.logging.AmpelLogger import AmpelLogger
from ampel.logging.LoggingUtils import LoggingUtils
from ampel.common.GraphiteFeeder import GraphiteFeeder
from ampel.config.AmpelConfig import AmpelConfig

class T3Controller(Schedulable):
	"""
	"""

	@staticmethod
	def load_job_configs(include=None, exclude=None):
		"""
		:param include: sequence of job names to explicitly include. If
		    specified, any job name not in this sequence will be excluded.
		:param exclude: sequence of job names to explicitly exclude. If
		    specified, any job name in this sequence will be excluded.
		"""
		job_configs = {}
		for key, klass in [('t3Jobs', T3JobConfig), ('t3Tasks', T3TaskConfig)]:
			for job_name, job_dict in AmpelConfig.get_config(key).items():
				if (include and job_name not in include) or (exclude and job_name in exclude):
					continue
				config = klass(**job_dict)
				if getattr(config, 'active', True):
					job_configs[job_name] = config

		return job_configs


	def __init__(self, t3_job_names=None, skip_jobs=set()):
		"""
		t3_job_names: optional list of strings. 
		If specified, only job with matching the provided names will be run.
		skip_jobs: optional list of strings. 
		If specified, jobs in this list will not be run.
		"""

		super(T3Controller, self).__init__()

		# Setup logger
		self.logger = AmpelLogger.get_unique_logger()
		self.logger.info("Setting up T3Controller")

		# Load job configurations
		self.job_configs = T3Controller.load_job_configs(t3_job_names, skip_jobs)

		schedule = ScheduleEvaluator()
		self._processes = {}
		for name, job_config in self.job_configs.items():
			for appointment in job_config.get('schedule'):
				if appointment is not None:
					schedule(self.scheduler, appointment).do(
						self.launch_t3_job, job_config
					).tag(name)

		self.scheduler.every(5).minutes.do(self.monitor_processes)


	def launch_t3_job(self, job_config):
		""" """
		if self.process_count > 5:
			self.logger.warn("{} processes are still lingering".format(self.process_count))
		
		# NB: we defer instantiation of T3Job to the subprocess to avoid
		# creating multiple MongoClients in the master process
		proc = Process(target=self._run_t3_job, args=(job_config,))
		proc.start()
		self._processes[proc.pid] = proc
		return proc


	def _run_t3_job(self, job_config, **kwargs):
		""" """
		name = getattr(job_config, 'job' if isinstance(job_config, T3JobConfig) else 'task')
		klass = T3Job if isinstance(job_config, T3JobConfig) else T3Task
		try:
			job = klass(job_config)
		except Exception as e:
			LoggingUtils.report_exception(
				self.logger, e, tier=3, info={
					'job': name,
				}
			)
			raise e
		return job.run(**kwargs)


	@property
	def process_count(self):
		""" """
		items = list(self._processes.items())
		if items is None:
			return 0
		for pid, proc in items:
			if proc.exitcode is not None:
				proc.join()
				del self._processes[pid]
		return len(self._processes)


	def join(self):
		""" """
		while self.process_count > 0:
			time.sleep(1)


	def monitor_processes(self):
		"""
		"""
		feeder = GraphiteFeeder(
			AmpelConfig.get_config('resources.graphite.default')
		)
		stats = {'processes': self.process_count}

		feeder.add_stats(stats, 't3.jobs')
		feeder.send()

		return stats
