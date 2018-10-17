#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/t3/T3Controller.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 26.02.2018
# Last Modified Date: 23.07.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import schedule, time, threading, logging, json
from types import MappingProxyType
from multiprocessing import Process
from ampel.pipeline.t3.T3Job import T3Job
from ampel.pipeline.config.t3.T3JobConfig import T3JobConfig
from ampel.pipeline.config.t3.T3TaskConfig import T3TaskConfig
from ampel.pipeline.common.Schedulable import Schedulable
from ampel.pipeline.config.t3.ScheduleEvaluator import ScheduleEvaluator
from ampel.pipeline.logging.AmpelLogger import AmpelLogger
from ampel.pipeline.common.GraphiteFeeder import GraphiteFeeder
from ampel.pipeline.config.AmpelConfig import AmpelConfig
from ampel.pipeline.common.AmpelUnitLoader import AmpelUnitLoader

class T3Controller(Schedulable):
	"""
	"""

	@staticmethod
	def load_job_configs(t3_job_names=None):
		"""
		"""
		job_configs = {}

		for job_name, job_dict in AmpelConfig.get_config("t3Jobs").items():

			if t3_job_names is not None and job_name not in t3_job_names:
				continue
			
			job_configs[job_name] = T3JobConfig(**job_dict)

		return job_configs

	def __init__(self, t3_job_names=None):
		"""
		t3_job_names: optional list of strings. 
		If specified, only job with matching the provided names will be run.
		"""

		super(T3Controller, self).__init__()

		# Setup logger
		self.logger = AmpelLogger.get_unique_logger()
		self.logger.info("Setting up T3Controller")

		# Load job configurations
		self.job_configs = T3Controller.load_job_configs(t3_job_names, self.logger)

		schedule = ScheduleEvaluator()
		self._processes = {}
		for name, job_config in self.job_configs.items():
			for appointment in job_config.get('schedule'):
				schedule(self.scheduler, appointment).do(self.launch_t3_job, job_config).tag(name)

		self.scheduler.every(5).minutes.do(self.monitor_processes)

	def launch_t3_job(self, job_config):
		if self.process_count > 5:
			self.logger.warn("{} processes are still lingering".format(self.process_count))
		
		# NB: we defer instantiation of T3Job to the subprocess to avoid
		# creating multiple MongoClients in the master process
		proc = Process(target=self._run_t3_job, args=(job_config,))
		proc.start()
		self._processes[proc.pid] = proc
		return proc

	def _run_t3_job(self, job_config, **kwargs):
		job = T3Job(job_config)
		return job.run(**kwargs)

	@property
	def process_count(self):
		""" """
		for pid, proc in self._processes.items():
			if proc.exitcode is not None:
				proc.join()
				del self._processes[pid]
		return len(self._processes)

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

def run(args):
	"""Run tasks at configured intervals"""
	T3Controller().run()

def list(args):
	"""List configured tasks"""
	jobs = AmpelConfig.get_config('t3Jobs')
	labels = {name: [t.get('task') for t in job['tasks']] for name, job in jobs.items()}
	columns = max([len(k) for k in labels.keys()]), max([max([len(k) for k in tasks]) for tasks in labels.values()])
	template = "{{:{}s}} {{:{}s}}".format(*columns)
	print(template.format('Job', 'Tasks'))
	print(template.format('='*columns[0], '='*columns[1]))
	for job, tasks in labels.items():
		for task in tasks:
			print(template.format(job,task))
			job = ''
		print(template.format('-'*columns[0], '-'*columns[1]))

class FrozenEncoder(json.JSONEncoder):
	def default(self, obj):
		if isinstance(obj, MappingProxyType):
			return dict(obj)
		elif isinstance(obj, T3TaskConfig):
			return dict(obj.task_doc)
		return super(FrozenEncoder, self).default(obj)

def show(args):
	"""Display job and task configuration"""
	job_doc = AmpelConfig.get_config('t3Jobs.{}'.format(args.job))
	if args.task is None:
		print(FrozenEncoder(indent=1).encode(job_doc))
	else:
		task = next(t for t in job_doc['tasks'] if t['task'] == args.task)
		print(FrozenEncoder(indent=1).encode(task))

def runjob(args):
	job_config = T3JobConfig(**AmpelConfig.get_config('t3Jobs.{}'.format(args.job)))
	job = T3Job(job_config, full_console_logging=True)
	job.run()

def rununit(args):
	"""
	Run a single T3 unit
	"""
	job_doc = {
		"job": "rununit",
		"active": True,
		"schedule": "every().sunday",
		"transients": {
			"state": "$latest",
			"select": {
				"created": {
					"after": {
						"use": "$timeDelta",
						"arguments": {
							"days": args.created
						}
					}
				},
				"modified": {
					"after": {
						"use": "$timeDelta",
						"arguments": {
							"days": args.modified
						}
					}
				},
				"channels": {'anyOf': args.channels},
				"withFlags": "INST_ZTF",
				"withoutFlags": "HAS_ERROR"
			},
			"content": {
				"docs": [
					"TRANSIENT",
					"COMPOUND",
					"T2RECORD",
					"PHOTOPOINT",
					"UPPERLIMIT"
				],
				"verbose": False
			},
			"chunk": 200
		},
		"tasks": [
			{
				"task": "rununit.task",
				"unitId": args.unit,
				"runConfig": args.runconfig,
				"updateJournal": False
			}
		]
	}
	job_config = T3JobConfig(**job_doc)
	# Record logs and exceptions in the db only if the run itself will be recorded
	report_exceptions = args.update_run_col
	job = T3Job(job_config, full_console_logging=True, db_logging=args.update_run_col,
	    update_events=args.update_run_col, update_tran_journal=args.update_tran_journal,
	    raise_exc=not args.update_run_col)
	job.run()

def get_required_resources():
	
	units = set()
	for job in T3Controller.load_job_configs().values():
		for task in job.tasks:
			units.add(task.unitId)
	resources = set()
	for unit in units:
		for resource in AmpelUnitLoader.get_class(3, unit).resources:
			resources.add(resource)
	return resources

def main():

	from ampel.pipeline.config.AmpelArgumentParser import AmpelArgumentParser
	from argparse import SUPPRESS
	import sys

	parser = AmpelArgumentParser(add_help=False)
	parser.require_resource('mongo', ['writer', 'logger'])
	parser.require_resource('graphite')
	# partially parse command line to get config
	opts, argv = parser.parse_known_args()
	# flesh out parser with resources required by t3 units
	parser.require_resources(*get_required_resources())

	subparsers = parser.add_subparsers(help='command help')
	subparser_list = []
	def add_command(f, name=None):
		if name is None:
			name = f.__name__
		p = subparsers.add_parser(name, help=f.__doc__, add_help=False)
		p.set_defaults(func=f)
		subparser_list.append(p)
		return p
	
	p = add_command(run)

	p = add_command(runjob)
	p.add_argument('job')

	p = add_command(runtask)
	p.add_argument('job')
	p.add_argument('task')
	p.add_argument('--created')
	p.add_argument('--modified')

	p = add_command(rununit)
	p.add_argument('unit')
	p.add_argument('--runconfig', default=None)
	p.add_argument('--update-run-col', default=False, action="store_true", help="Record this run in the jobs collection")
	p.add_argument('--update-tran-journal', default=False, action="store_true", help="Record this run in the transient journal")
	p.add_argument('--channels', nargs='+', default=[])
	p.add_argument('--created', type=int, default=-40)
	p.add_argument('--modified', type=int, default=-1)

	p = add_command(list)

	p = add_command(show)
	p.add_argument('job')
	p.add_argument('task', nargs='?', default=None)
	
	opts, argv = parser.parse_known_args()
	if hasattr(opts, 'func') and opts.func == rununit:
		parser.require_resources(*AmpelUnitLoader.get_class(3, opts.unit).resources)

	# Now that side-effect-laden parsing is done, add help
	for p in [parser] + subparser_list:
		p.add_argument('-h', '--help', action="help", default=SUPPRESS, help="show this message and exit")
	opts = parser.parse_args()
	opts.func(opts)
