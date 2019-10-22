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
from functools import partial
import multiprocessing
from ampel.pipeline.t3.T3Job import T3Job
from ampel.pipeline.t3.T3Task import T3Task
from ampel.pipeline.config.t3.T3JobConfig import T3JobConfig
from ampel.pipeline.config.t3.T3TaskConfig import T3TaskConfig
from ampel.pipeline.common.Schedulable import Schedulable
from ampel.pipeline.config.t3.ScheduleEvaluator import ScheduleEvaluator
from ampel.pipeline.logging.AmpelLogger import AmpelLogger
from ampel.pipeline.logging.LoggingUtils import LoggingUtils
from ampel.pipeline.common.GraphiteFeeder import GraphiteFeeder
from ampel.pipeline.config.AmpelConfig import AmpelConfig
from ampel.pipeline.common.AmpelUnitLoader import AmpelUnitLoader

log = logging.getLogger(__name__)

class T3Controller(Schedulable):
	"""
	"""

	@staticmethod
	def load_job_configs(include=None, exclude=None, exclude_units=None):
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
				if exclude_units:
					if klass is T3TaskConfig and config.unitId in exclude_units:
						continue
					elif klass is T3JobConfig:
						config.tasks = [t for t in config.tasks if not t.unitId in exclude_units]
						if not config.tasks:
							continue
				if getattr(config, 'active', True):
					job_configs[job_name] = config

		return job_configs

	def __init__(self, t3_job_names=None, skip_jobs=set(), exclude_units=set()):
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
		self.job_configs = T3Controller.load_job_configs(t3_job_names, skip_jobs, exclude_units)

		schedule = ScheduleEvaluator()
		self._pool = multiprocessing.get_context('spawn').Pool(maxtasksperchild=1,)
		self._tasks = []
		for name, job_config in self.job_configs.items():
			for appointment in job_config.get('schedule'):
				schedule(self.scheduler, appointment).do(self.launch_t3_job, job_config).tag(name)

		self.scheduler.every(5).minutes.do(self.monitor_processes)

	def launch_t3_job(self, job_config):
		if self.process_count > 5:
			self.logger.warn("{} processes are still lingering".format(self.process_count))
		
		# NB: we defer instantiation of T3Job to the subprocess to avoid
		# creating multiple MongoClients in the master process
		fut = self._pool.apply_async(self._run_t3_job,
		    args=(
		        AmpelConfig.recursive_unfreeze(AmpelConfig.get_config()),
		        job_config,
		))
		self._tasks.append(fut)
		return fut

	@staticmethod
	def _run_t3_job(ampel_config, job_config, **kwargs):
		AmpelConfig.set_config(ampel_config)
		name = getattr(job_config, 'job' if isinstance(job_config, T3JobConfig) else 'task')
		klass = T3Job if isinstance(job_config, T3JobConfig) else T3Task
		try:
			job = klass(job_config)
		except Exception as e:
			LoggingUtils.report_exception(
				AmpelLogger.get_unique_logger(), e, tier=3, info={
					'job': name,
				}
			)
			raise e
		return job.run(**kwargs)

	@property
	def process_count(self):
		""" """
		pending = []
		for fut in self._tasks:
			if fut.ready():
				fut.get()
			else:
				pending.append(fut)
		self._tasks = pending
		return len(self._tasks)

	def join(self):
		self._pool.close()
		self._pool.join()

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
	T3Controller(args.jobs, args.skip_jobs, args.skip_units).run()

# pylint: disable=bad-builtin
def list_tasks(args):
	"""List configured tasks"""
	jobs = AmpelConfig.get_config('t3Jobs')
	labels = {name: [(t.get('task'),t.get('unitId')) for t in job['tasks']] for name, job in jobs.items() if job.get('active', True)}
	if AmpelConfig.get_config('t3Tasks') is not None:
		tasks = [(t.get('task'), t.get('unitId')) for t in AmpelConfig.get_config('t3Tasks').values()]
		if len(tasks):
			labels['(channel tasks)'] = tasks
	columns = max([len(k) for k in labels.keys()]), max([max([len(k[0]) for k in tasks]) for tasks in labels.values()]), max([max([len(k[1]) for k in tasks]) for tasks in labels.values()])
	template = "{{:{}s}} {{:{}s}} {{:{}s}}".format(*columns)
	print(template.format('Job', 'Task', 'Unit'))
	print(template.format('='*columns[0], '='*columns[1], '='*columns[2]))
	for job, tasks in labels.items():
		for task,unit in tasks:
			print(template.format(job,task,unit))
			job = ''
		print(template.format('-'*columns[0], '-'*columns[1], '-'*columns[2]))

class FrozenEncoder(json.JSONEncoder):
	def default(self, obj):
		if isinstance(obj, MappingProxyType):
			return dict(obj)
		elif isinstance(obj, T3TaskConfig):
			return dict(obj.task_doc)
		return super(FrozenEncoder, self).default(obj)

# pylint: disable=bad-builtin
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
	if args.task is not None:
		job_config.tasks = [t for t in job_config.tasks if t.task == args.task]
	job = T3Job(job_config, full_console_logging=True, raise_exc=True)
	job.run()

def runtask(args):
	job_config = T3TaskConfig(**AmpelConfig.get_config('t3Tasks.{}'.format(args.task)))
	job = T3Task(job_config, full_console_logging=True, raise_exc=True)
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
							"days": -args.created
						}
					}
				},
				"modified": {
					"after": {
						"use": "$timeDelta",
						"arguments": {
							"days": -args.modified
						}
					}
				},
				"channels": {'anyOf': args.channels},
				"scienceRecords": [r.dict() for r in args.science_records] if args.science_records else None,
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
				]
			},
			"chunk": args.chunk
		},
		"tasks": [
			{
				"task": "rununit.task",
				"unitId": args.unit,
				"runConfig": getattr(args, 'runConfig', None)
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

def dryrun(args):
	def make_dry(task):
		if args.task and task.task != args.task:
			return False
		klass = AmpelUnitLoader.get_class(3, task.unitId)
		safe = hasattr(klass, 'RunConfig') and 'dryRun' in klass.RunConfig.__fields__
		if safe:
			if task.runConfig is None:
				task.runConfig = klass.RunConfig()
			task.runConfig.dryRun = True
			return True
		else:
			log.info('Task {} ({}) has no dryRun config'.format(task.task, klass.__name__))
			return False

	for config in T3Controller.load_job_configs([args.job] if args.job else None).values():
		if not isinstance(config, T3JobConfig):
			continue
		config.tasks = [t for t in config.tasks if make_dry(t)]
		if len(config.tasks) == 0:
			continue
		job = T3Job(config, full_console_logging=True, db_logging=False,
		    update_events=False, update_tran_journal=False,
		    raise_exc=True)
		try:
			job.run()
			log.info('Job {} complete'.format(config.job))
		except:
			log.error('Job {} failed'.format(config.job))
			raise

def get_required_resources():
	
	units = set()
	for job in T3Controller.load_job_configs().values():
		if isinstance(job, T3JobConfig):
			for task in job.tasks:
				units.add(task.unitId)
		else:
			task = job
			units.add(task.unitId)
	resources = set()
	for unit in units:
		for resource in AmpelUnitLoader.get_class(3, unit).resources:
			resources.add(resource)
	return resources

def main():

	from ampel.pipeline.config.t3.ScienceRecordMatchConfig import ScienceRecordMatchConfig
	from ampel.pipeline.config.AmpelArgumentParser import AmpelArgumentParser
	from argparse import SUPPRESS, Action, Namespace
	import sys

	parser = AmpelArgumentParser(add_help=False)
	parser.require_resource('mongo', ['writer', 'logger'])
	parser.require_resource('graphite')
	# partially parse command line to get config
	opts, argv = parser.parse_known_args()
	# flesh out parser with resources required by t3 units
	parser.require_resources(*get_required_resources())

	subparsers = parser.add_subparsers(help='command help', dest='command')
	subparsers.required = True
	subparser_list = []
	def add_command(f, name=None):
		if name is None:
			name = f.__name__
		p = subparsers.add_parser(name, help=f.__doc__, add_help=False)
		p.set_defaults(func=f)
		subparser_list.append(p)
		return p
	
	p = add_command(run)
	p.add_argument('--jobs', nargs='+', default=None, help='run only these jobs')
	p.add_argument('--skip-jobs', nargs='+', default=None, help='do not run these jobs')
	p.add_argument('--skip-units', nargs='+', default=None, help='do not run tasks that use these units')

	p = add_command(runjob)
	p.add_argument('job')
	p.add_argument('task', nargs='?')

	p = add_command(runtask)
	p.add_argument('task')

	p = add_command(dryrun)
	p.add_argument('job', nargs='?')
	p.add_argument('task', nargs='?')

	p = add_command(rununit)
	p.add_argument('unit')
	p.add_argument('--update-run-col', default=False, action="store_true", help="Record this run in the jobs collection")
	p.add_argument('--update-tran-journal', default=False, action="store_true", help="Record this run in the transient journal")
	p.add_argument('--channels', nargs='+', default=[], help="Select transients in any of these channels")
	p.add_argument('--science-records', nargs='+', default=None,
	    type=ScienceRecordMatchConfig.parse_raw,
	    help="Filter based on transient records. The filter should be the JSON representation of a ScienceRecordMatchConfig")
	p.add_argument('--chunk', type=int, default=200, help="Provide CHUNK transients at a time")
	p.add_argument('--created', type=int, default=40, help="Select transients created in the last CREATED days")
	p.add_argument('--modified', type=int, default=1, help="Select transients modified in the last MODIFIED days")

	p = add_command(list_tasks, 'list')

	p = add_command(show)
	p.add_argument('job')
	p.add_argument('task', nargs='?', default=None)
	
	opts, argv = parser.parse_known_args()
	if opts.command == 'rununit':
		klass = AmpelUnitLoader.get_class(3, opts.unit)
		parser.require_resources(*klass.resources)
		if hasattr(klass, 'RunConfig'):
			p = subparsers.choices[opts.command]
			class GroupAction(Action):
				def __call__(self, parser, namespace, values, option_string=None):
					group,dest = self.dest.split('.',2)
					groupspace = getattr(namespace, group, dict())
					groupspace[dest] = values
					setattr(namespace, group, groupspace)
			class YesNoGroupAction(GroupAction):
				def __init__(self, option_strings, dest=None, default=True, **kwargs):
					name = option_strings[0].lstrip('-')
					super(YesNoGroupAction, self).__init__(
					    [
					       name,
					        '--no-' + name,
					    ] + option_strings[1:],
					    dest=(name.replace('-', '_') if dest is None else dest),
					    nargs=0, const=None, default=default, **kwargs)
				def __call__(self, parser, namespace, values, option_string=None):
					if option_string.startswith('--no-'):
						v = False
					else:
						v = True
					super(YesNoGroupAction, self).__call__(parser, namespace, v, option_string)
			def validate_union(v, types):
				for t in types:
					try:
						return validate(t)(v)
					except:
						pass
				raise TypeError
			def validate(typus):
				if repr(type(typus)) == 'typing.Union':
					return partial(validate_union, types=typus.__args__)
				elif hasattr(typus, 'parse_raw'):
					return lambda v: typus.parse_raw(v).dict()
				else:
					return typus
			for f in klass.RunConfig.__fields__.values():
				validator = validate(f.type_)
				if f.type_ is bool:
					action = YesNoGroupAction
				else:
					action = GroupAction
				if f.required:
					p.add_argument('runConfig.'+f.name, type=validator, 
					    action=action, metavar=f.name)
				else:
					p.add_argument('--'+f.name, dest='runConfig.'+f.name, type=validator,
					    default=f.default, action=action, metavar=f.name.upper(), help="{} parameter".format(opts.unit))

	# Now that side-effect-laden parsing is done, add help
	for p in [parser] + subparser_list:
		p.add_argument('-h', '--help', action="help", default=SUPPRESS, help="show this message and exit")
	opts = parser.parse_args()
	opts.func(opts)
