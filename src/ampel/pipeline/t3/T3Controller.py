#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/t3/T3Controller.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 26.02.2018
# Last Modified Date: 23.07.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import schedule, time, threading
from ampel.pipeline.t3.T3Job import T3Job
from ampel.pipeline.t3.T3JobConfig import T3JobConfig
from ampel.pipeline.common.Schedulable import Schedulable
from ampel.pipeline.logging.LoggingUtils import LoggingUtils
from ampel.pipeline.common.GraphiteFeeder import GraphiteFeeder
from ampel.pipeline.config.AmpelConfig import AmpelConfig

class T3Controller(Schedulable):
	"""
	"""

	@staticmethod
	def load_job_configs(t3_job_names, logger):
		"""
		"""
		job_configs = {}

		for job_name in AmpelConfig.get_config("t3_jobs").keys():

			if t3_job_names is None:
				logger.info("Ignoring job without name")
				continue

			if job_name in t3_job_names:
				logger.info("Ignoring job '%s' as requested" % job_name)
				continue

			job_configs[job_name] = T3JobConfig.load(job_name, logger)

		return job_configs


	@staticmethod
	def get_required_resources(t3_job_names=None):
		"""
		"""
		logger = LoggingUtils.get_logger(unique=True)
		resources = set()

		for job_config in T3Controller.load_job_configs(t3_job_names, logger).values():
			for task_config in job_config.get_task_configs():
				resources.update(task_config.t3_unit_class.resources)

		return resources


	def __init__(self, t3_job_names=None):
		"""
		t3_job_names: optional list of strings. 
		If specified, only job with matching the provided names will be run.
		"""

		super(T3Controller, self).__init__()

		# Setup logger
		self.logger = LoggingUtils.get_logger(unique=True)
		self.logger.info("Setting up T3Controler")

		# Load job configurations
		self.job_configs = T3Controller.load_job_configs(t3_job_names, self.logger)

		for job_config in self.job_configs.values():
			job_config.schedule_job(self.scheduler)

		self.scheduler.every(5).minutes.do(self.monitor_processes)


	def monitor_processes(self):
		"""
		"""
		feeder = GraphiteFeeder(
			AmpelConfig.get_config('resources.graphite.default')
		)
		stats = {}

		for job_name, job_config in self.job_configs.items():
			stats[job_name] = {'processes': job_config.process_count}

		feeder.add_stats(stats, 't3.jobs')
		feeder.send()

		return stats


def run():

	from ampel.pipeline.config.ConfigLoader import AmpelArgumentParser

	parser = AmpelArgumentParser()
	parser.require_resource('mongo', ['writer', 'logger'])
	parser.require_resource('graphite')
	# partially parse command line to get config
	opts, argv = parser.parse_known_args()
	# flesh out parser with resources required by t3 units
	parser.require_resources(*T3Controller.get_required_resources())
	# parse again, filling the resource config
	opts = parser.parse_args()

	T3Controller().run()
