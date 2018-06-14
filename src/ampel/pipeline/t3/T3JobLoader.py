#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/t3/T3JobLoader.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 07.06.2018
# Last Modified Date: 12.06.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from functools import reduce
from voluptuous import Schema, Required, Any, Optional, ALLOW_EXTRA

from ampel.pipeline.t3.T3Job import T3Job
from ampel.pipeline.t3.T3TaskLoader import T3TaskLoader
from ampel.pipeline.t3.TimeConstraint import TimeConstraint
from ampel.pipeline.common.AmpelUtils import AmpelUtils
from ampel.pipeline.logging.LoggingUtils import LoggingUtils

class T3JobLoader:
	"""
	"""

	_docs_schema = Schema(
		Any("TRANSIENT", "PHOTOPOINT", "UPPERLIMIT", "COMPOUND", "T2RECORD")
	)

	job_schema = Schema(
		{
			Required('active'): bool,
			Required('schedule'): Any(
				{
					Required('mode'): 'fixed_time',
					Required('time'): str
				},
				{
					Required('mode'): 'fixed_rate',
					Required('interval'): int
				}
			),
			Required('task(s)'): Any(
				T3TaskLoader.t3_task_schema, 
				[T3TaskLoader.t3_task_schema]
			),
			'input': {
				Required('select'): {
					'created': TimeConstraint.schema_types,
					'modified': TimeConstraint.schema_types,
					'channel(s)': Any(str, [str]),
					'withFlag(s)': Any(str, [str]),
					'withoutFlag(s)': Any(str, [str])
				},
				Required('load'): {
					Required('state(s)'): Any("all", "latest"),
					'doc(s)': Any(_docs_schema, [_docs_schema]),
					't2(s)': Any(str, [str]),
					'verbose': bool
				},
				'chunk': int
			},
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
	def load(cls, al_config, job_name, logger=None):
		"""
		This class is mainly made of config validity tests.
		NOTE: channel name existence is not checked on purpose.
		"""

		if logger is None:
			logger = LoggingUtils.get_logger()

		if al_config['t3_jobs'].get(job_name) is None:
			raise ValueError("Job %s not found" % job_name)

		logger.info("Loading job %s" % job_name)
		job_doc = cls.job_schema(al_config['t3_jobs'].get(job_name))
		input_select = cls.get_config(job_doc, 'input.select')
		t3_tasks = []
		all_tasks_sels = {} 

		# Robustness
		if input_select is not None:

			if ((type(input_select.get('doc(s)')) is list and "T2RECORD" not in input_select['doc(s)']) or
				(type(input_select.get('doc(s)')) is str and "T2RECORD" != input_select['doc(s)'])
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

		# Load and check tasks
		for task_doc in job_doc['task(s)']:
			t3_tasks.append(
				T3TaskLoader.load(al_config, job_name, task_doc['name'], all_tasks_sels, logger)
			)

		return T3Job(job_name, job_doc, t3_tasks)

	
	@staticmethod
	def get_config(job_doc, param_name):
		"""
		"""
		return reduce(dict.get, param_name.split("."), job_doc)
