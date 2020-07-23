#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/run/t3.py
# License           : BSD-3-Clause
# Author            : jvs
# Date              : Unspecified
# Last Modified Date: 20.08.2019
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import json, logging
from types import MappingProxyType
from functools import partial
from ampel.t3.T3Job import T3Job
from ampel.t3.T3Task import T3Task
from ampel.t3.T3Controller import T3Controller
from ampel.config.t3.T3JobConfig import T3JobConfig
from ampel.config.t3.T3TaskConfig import T3TaskConfig
from ampel.config.AmpelConfig import AmpelConfig
from ampel.abstract.UnitLoader import UnitLoader

log = logging.getLogger(__name__)

def run(args):
	"""Run tasks at configured intervals"""
	T3Controller(args.jobs, args.skip_jobs).run()

# pylint: disable=bad-builtin
def list_tasks(args):
	"""List configured tasks"""
	jobs = AmpelConfig.get('job')
	labels = {name: [(t.get('task'),t.get('className')) for t in job['tasks']] for name, job in jobs.items() if job.get('active', True)}
	if AmpelConfig.get('task') is not None:
		tasks = [(t.get('task'), t.get('className')) for t in AmpelConfig.get('task').values()]
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
	job_doc = AmpelConfig.get('job.{}'.format(args.job))
	if args.task is None:
		print(FrozenEncoder(indent=1).encode(job_doc))
	else:
		task = next(t for t in job_doc['tasks'] if t['task'] == args.task)
		print(FrozenEncoder(indent=1).encode(task))

def runjob(args):
	job_config = T3JobConfig(**AmpelConfig.get('job.{}'.format(args.job)))
	if args.task is not None:
		job_config.tasks = [t for t in job_config.tasks if t.task == args.task]
	job = T3Job(job_config, full_console_logging=True, raise_exc=True)
	job.run()

def runtask(args):
	job_config = T3TaskConfig(**AmpelConfig.get('task.{}'.format(args.task)))
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
				"scienceRecords": [r.dict() for r in args.science_records],
				"withTags": "ZTF",
				"withoutTags": "HAS_ERROR"
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
				"className": args.unit,
				"run_config": getattr(args, 'run_config', None)
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
		klass = UnitLoader.get_class(3, task.className)
		safe = hasattr(klass, 'RunConfig') and 'dryRun' in klass.RunConfig.__fields__
		if safe:
			if task.run_config is None:
				task.run_config = klass.RunConfig()
			task.run_config.dryRun = True
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
				units.add(task.className)
		else:
			task = job
			units.add(task.className)
	resources = set()
	for unit in units:
		for resource in UnitLoader.get_class(3, unit).resources:
			resources.add(resource)
	return resources

def main():

	from ampel.config.t3.T2RecordMatchConfig import T2RecordMatchConfig
	from ampel.run.AmpelArgumentParser import AmpelArgumentParser
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
	    type=T2RecordMatchConfig.parse_raw,
	    help="Filter based on transient records. The filter should be the JSON representation of a T2RecordMatchConfig")
	p.add_argument('--chunk', type=int, default=200, help="Provide CHUNK transients at a time")
	p.add_argument('--created', type=int, default=40, help="Select transients created in the last CREATED days")
	p.add_argument('--modified', type=int, default=1, help="Select transients modified in the last MODIFIED days")

	p = add_command(list_tasks, 'list')

	p = add_command(show)
	p.add_argument('job')
	p.add_argument('task', nargs='?', default=None)
	
	opts, argv = parser.parse_known_args()
	if opts.command == 'rununit':
		klass = UnitLoader.get_class(3, opts.unit)
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
					p.add_argument('run_config.'+f.name, type=validator, 
					    action=action, metavar=f.name)
				else:
					p.add_argument('--'+f.name, dest='run_config.'+f.name, type=validator,
					    default=f.default, action=action, metavar=f.name.upper(), help="{} parameter".format(opts.unit))

	# Now that side-effect-laden parsing is done, add help
	for p in [parser] + subparser_list:
		p.add_argument('-h', '--help', action="help", default=SUPPRESS, help="show this message and exit")
	opts = parser.parse_args()
	opts.func(opts)
