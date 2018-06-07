#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/t3/T3Job.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 06.03.2018
# Last Modified Date: 07.06.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from ampel.pipeline.logging.LoggingUtils import LoggingUtils
from ampel.pipeline.common.AmpelUtils import AmpelUtils
from ampel.pipeline.t3.T3TaskLoader import T3TaskLoader
from ampel.pipeline.t3.T3Task import T3Task
from ampel.flags.TransientFlags import TransientFlags
from ampel.flags.FlagUtils import FlagUtils
from datetime import timedelta, datetime
from functools import reduce
from voluptuous import Schema, Required, Any, Optional, ALLOW_EXTRA

class T3Job:
	"""
	"""

	_docs_schema = Schema(Any("TRANSIENT", "PHOTOPOINT", "UPPERLIMIT", "COMPOUND", "T2RECORD"))

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
					'created': {
						'timedelta': dict, 
						'from': Any(
							{'unixTime': float},
							{'strTime': str, 'strFormat': str}
						), 
						'until': Any(
							{'unixTime': float},
							{'strTime': str, 'strFormat': str}
						)
					},
					'modified': {
						'timedelta': dict, 
						'from': Any(
							{'unixTime': float},
							{'strTime': str, 'strFormat': str}
						), 
						'until': Any(
							{'unixTime': float},
							{'strTime': str, 'strFormat': str}
						)
					},
					'channel(s)': Any(str, [str]),
					'withFlag(s)': Any(str, [str]),
					'withoutFlag(s)': Any(str, [str])
				},
				Required('load'): {
					Required('state'): Any("all", "latest"),
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


	def __init__(self, job_name, job_doc, t3_tasks):
		"""
		This class is mainly made of config validity tests.
		NOTE: channel name existence is not checked on purpose.
		"""

		self.job_name = job_name
		self.job_doc = job_doc
		self.t3_tasks = t3_tasks


	def get_tasks(self):
		""" 
		Returns the loaded tasks associated with this job
		"""
		return self.t3_tasks


	def get_chunk(self):
		""" """
		return self.get_config('input.chunk')


	def get_config(self, param_name):
		"""
		"""
		return reduce(dict.get, param_name.split("."), self.job_doc)


	def launch_t3_job(self, bla):
		#run_job(self.al_config, central_db, t3_job, logger)
		pass


	def schedule(self, scheduler):

		t3_job = None
		if self.get_config('schedule.mode') == "fixed_rate":

			scheduler.every(
				self.get_config('schedule.interval')
			).minutes.do(
				self.launch_t3_job, 
				t3_job
			)

		elif self.get_config('schedule.mode') == "fixed_time":

			scheduler.every().day.at(
				self.get_config('schedule.time')
			).do(
				self.launch_t3_job, 
				t3_job
			)
