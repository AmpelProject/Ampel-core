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
from ampel.pipeline.t3.T3Job import T3Job
from ampel.pipeline.t3.T3JobConfig import T3JobConfig, T3TaskConfig
from ampel.pipeline.common.Schedulable import Schedulable
from ampel.pipeline.logging.AmpelLogger import AmpelLogger
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

		for job_name in AmpelConfig.get_config("t3Jobs").keys():

			if t3_job_names is not None and job_name in t3_job_names:
				logger.info("Ignoring job '%s' as requested" % job_name)
				continue

			job_configs[job_name] = T3JobConfig.load(job_name, logger)

		return job_configs


	@staticmethod
	def get_required_resources(t3_job_names=None):
		"""
		"""
		logger = AmpelLogger.get_unique_logger()
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
		self.logger = AmpelLogger.get_unique_logger()
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

def run(args):
	"""Run tasks at configured intervals"""
	T3Controller().run()

def list(args):
	"""List configured tasks"""
	jobs = T3Controller.load_job_configs(None, None)
	labels = {name: [t.get('name') for t in job.get_task_configs()] for name, job in jobs.items()}
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
	job = T3JobConfig.load(args.job)
	if args.task is None:
		print(FrozenEncoder(indent=1).encode(job.job_doc))
	else:
		task = next(t for t in job.get_task_configs() if t.get('name') == args.task)
		print(FrozenEncoder(indent=1).encode(task))

def runjob(args):
	job_config = T3JobConfig.load(args.job)
	job = T3Job(job_config, full_console_logging=True)
	job.run()

def runtask(args):
	job_config = T3JobConfig.load(args.job)
	job_config.t3_task_configs = [t for t in job_config.get_task_configs() if t.get('name') == args.task]
	job = T3Job(job_config, full_console_logging=False)
	for param in 'created', 'modified':
		if getattr(args, param) is not None:
			job.overwrite_parameter(param, getattr(args, param))

def rununit(args):
	"""
	Run a single T3 unit
	"""
	job_doc = {
		"active": True,
		"schedule": "manual",
		"input": {
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
				"channel(s)": args.channels,
				"withFlag(s)": "INST_ZTF",
				"withoutFlag(s)": "HAS_ERROR"
			},
			"load": {
				"state": "$latest",
				"doc(s)": [
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
		"task(s)": [
			{
				"name": "FroopyFlarj",
				"t3Unit": args.unit,
				"runConfig": args.runconfig,
				"updateJournal": False
			}
		]
	}
	job_config = T3JobConfig.from_doc("froopydyhoop", job_doc)
	# Record logs in the db only if the run itself will be recorded
	logger = logging.getLogger(__name__) if not args.update_run_col else None
	# Likewise, only catch exceptions if the run is being recorded
	report_exceptions = logger is None
	job = T3Job(job_config, full_console_logging=True, logger=logger)
	job.run(args.update_run_col, args.update_tran_journal, report_exceptions)

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
	parser.require_resources(*T3Controller.get_required_resources())

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
		unit = T3TaskConfig.get_t3_unit(opts.unit, logging.getLogger(__name__))
		parser.require_resources(*unit.resources)

	# Now that side-effect-laden parsing is done, add help
	for p in [parser] + subparser_list:
		p.add_argument('-h', '--help', action="help", default=SUPPRESS, help="show this message and exit")
	opts = parser.parse_args()
	opts.func(opts)
