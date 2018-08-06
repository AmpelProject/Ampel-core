#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/t3/T3JobConfig.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 06.03.2018
# Last Modified Date: 04.08.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from functools import reduce
from types import MappingProxyType
from multiprocessing import Process
from voluptuous import Schema, Required, Any, Optional, ALLOW_EXTRA

from ampel.pipeline.common.AmpelUtils import AmpelUtils
from ampel.pipeline.config.AmpelConfig import AmpelConfig
from ampel.pipeline.logging.LoggingUtils import LoggingUtils
from ampel.pipeline.t3.T3TaskConfig import T3TaskConfig
from ampel.pipeline.t3.TimeConstraint import TimeConstraint
from ampel.pipeline.t3.T3Job import T3Job


class T3JobConfig:
	"""
	"""

	_ampel_docs = Schema(
		Any("TRANSIENT", "PHOTOPOINT", "UPPERLIMIT", "COMPOUND", "T2RECORD")
	)

	# static schema used for dict/json validation
	job_schema = Schema(
		{
			Required('active'): bool,
			Required('schedule'): Any(
				{
					Required('mode'): 'fixed_time',
					Required('timeStr'): str,
					Required('timeFormat'): str
				},
				{
					Required('mode'): 'fixed_rate',
					Required('interval'): int
				},
				'manual'
			),
			Required('task(s)'): Any(
				T3TaskConfig.t3_task_schema, 
				[T3TaskConfig.t3_task_schema]
			),
			'input': {
				Required('select'): {
					'created': TimeConstraint.schema,
					'modified': TimeConstraint.schema,
					'channel(s)': Any(str, [str]),
					'withFlag(s)': Any(str, [str]),
					'withoutFlag(s)': Any(str, [str])
				},
				Required('load'): {
					Required('state'): Any("$all", "$latest"),
					'doc(s)': Any(_ampel_docs, [_ampel_docs]),
					't2(s)': Any(str, [str]),
					'verbose': bool
				},
				'chunk': int
			},
			Optional('globalInfo', default=False): bool,
			'onError': {
				'sendMail': {
					Required('to'): str,
					Required('excStack'): bool
				},
				Optional('stopAmpel', default=False): bool,
				Optional('retry', default=False): bool
			}
		}, 
		extra=ALLOW_EXTRA
	)


	@classmethod
	def load(cls, job_name, logger=None):
		"""
		This class is mainly made of config validity tests.
		NOTE: channel name existence is not checked on purpose.
		"""

		if logger is None:
			logger = LoggingUtils.get_logger()

		# Get, check and set defaults of t3 task doc 
		job_doc = AmpelConfig.get_config(
			't3_jobs.%s' % job_name, 
			cls.job_schema
		)

		# Robustness
		if job_doc is None:
			raise ValueError("Job %s not found" % job_name)

		logger.info("Loading job %s" % job_name)
		t3_task_configs = []
		all_tasks_sels = {} 

		# Robustness
		docs_sel = AmpelUtils.get_by_path(job_doc, 'input.select.doc(s)')
		if docs_sel is not None:
			if (
				(AmpelUtils.is_sequence(docs_sel) and "T2RECORD" not in docs_sel) or
				(type(docs_sel) is str and "T2RECORD" != docs_sel)
			):
				raise ValueError(
					"T3 job %s config error: T2RECORD must be in input->select->doc(s) "+
					"when input->select->t2(s) filtering is configured" %
					job_name
				)

		# Build set of channel(s)/t2(s)/doc(s) for all tasks combined
		for task_doc in job_doc['task(s)']:

			if 'select' not in task_doc:
				continue

			for key in ('channel(s)', 't2(s)', 'doc(s)'):
				if task_doc['select'].get(key) is not None:
					if key not in all_tasks_sels:
						all_tasks_sels[key] = set()
					all_tasks_sels[key].update(
						AmpelUtils.to_set(task_doc['select'][key])
					)

		# Check TaskConfigS rightness
		if 'channel(s)' in all_tasks_sels and '$forEach' in all_tasks_sels['channel(s)']:
			# Either none or all tasks must make use of the $forEach operator
			if len(all_tasks_sels['channel(s)']) != 1:
				raise ValueError("Illegal task sub-channel selection")

		# Load and check each individual Task
		for task_doc in job_doc['task(s)']:
			t3_task_configs.append(
				T3TaskConfig.load(job_name, task_doc['name'], all_tasks_sels, logger)
			)

		# Create JobConfig
		return T3JobConfig(job_name, job_doc, t3_task_configs)

	
	def __init__(self, job_name, job_doc, t3_task_configs):
		"""
		job_name: string
		job_doc: dict instance
		t3_task_bodies: list of instances of ampel.pipeline.t3.T3Task
		"""

		self.job_name = job_name
		self.job_doc = job_doc
		self.t3_task_configs = t3_task_configs
		self._processes = {}


	def get_task_configs(self):
		""" 
		Returns the loaded task configurations associated with this job
		"""
		return self.t3_task_configs


	def get(self, param_name):
		""" """
		return AmpelUtils.get_by_path(self.job_doc, param_name)


	@property
	def process_count(self):
		""" """
		for pid, proc in list(self._processes.items()):
			if proc.exitcode is not None:
				proc.join()
				del self._processes[pid]
		return len(self._processes)


	def launch_t3_job(self):
		""" """
		# TODO: log or warn about a large number of lingering processes here
		for pid, proc in list(self._processes.items()):
			if proc.exitcode is not None:
				del self._processes[pid]
		
		proc = Process(target=self.run)
		proc.start()
		self._processes[proc.pid] = proc
		return proc


	def run(self, central_db=None, logger=None):
		""" """
		T3Job(self, central_db, logger).run()


	def schedule_job(self, scheduler):

		if self.get('schedule.mode') == "fixed_rate":

			scheduler.every(
				self.get('schedule.interval')
			).minutes.do(
				self.launch_t3_job
			)

		elif self.get('schedule.mode') == "fixed_time":

			scheduler.every().day.at(
				self.get('schedule.timeStr')
			).do(
				self.launch_t3_job
			)
