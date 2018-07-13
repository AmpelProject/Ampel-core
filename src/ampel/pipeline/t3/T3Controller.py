#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/t3/T3Controller.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 26.02.2018
# Last Modified Date: 09.07.2018
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

	def __init__(self, t3_job_names=None):
		"""
		't3_job_names': optional list of strings. 
		If specified, only job with these names will be run.
		"""
		super(T3Controller, self).__init__()
		# Setup logger
		self.logger = LoggingUtils.get_logger(unique=True)
		self.logger.info("Setting up T3Controler")
		self.jobs = {}

		for job_name in AmpelConfig.get_config("t3_jobs").keys():

			if t3_job_names is not None and job_name not in t3_job_names:
				continue

			job_config = T3JobConfig.load(job_name, self.logger)
			job_config.schedule_job(self.scheduler)
			self.jobs[job_name] = job_config
		
		self.scheduler.every(5).minutes.do(self.monitor_processes)


	def monitor_processes(self):
		"""
		"""
		feeder = GraphiteFeeder(AmpelConfig.get_config('resources.graphite'))
		stats = {}
		for job_name, job_config in self.jobs.items():
			stats[job_name] = {'processes': job_config.process_count}
		feeder.add_stats(stats, 't3.jobs')
		feeder.send()
		return stats


def run():

	from ampel.pipeline.config.ConfigLoader import AmpelArgumentParser

	parser = AmpelArgumentParser()
	parser.require_resource('mongo', ['writer'])
	parser.require_resource('graphite')
	opts = parser.parse_args()
	T3Controller().run()
