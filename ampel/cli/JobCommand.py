#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/cli/JobCommand.py
# License:             BSD-3-Clause
# Author:              valery brinnel <firstname.lastname@gmail.com>
# Date:                15.03.2021
# Last Modified Date:  22.10.2022
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

import tarfile, tempfile, ujson, yaml, io, os, signal, sys, \
	subprocess, platform, shutil, filecmp, psutil
from time import time, sleep
from multiprocessing import Queue, Process
from argparse import ArgumentParser
from importlib import import_module
from typing import Any
from collections.abc import Sequence
from urllib.request import urlretrieve
from ampel.abstract.AbsEventUnit import AbsEventUnit
from ampel.abstract.AbsProcessorTemplate import AbsProcessorTemplate
from ampel.model.UnitModel import UnitModel
from ampel.core.EventHandler import EventHandler
from ampel.dev.DevAmpelContext import DevAmpelContext
from ampel.log.AmpelLogger import AmpelLogger
from ampel.log.LogFlag import LogFlag
from ampel.util.freeze import recursive_freeze
from ampel.util.hash import build_unsafe_dict_id
from ampel.util.distrib import get_dist_names
from ampel.util.collections import try_reduce
from ampel.cli.config import get_user_data_config_path
from ampel.cli.utils import _maybe_int
from ampel.cli.AbsCoreCommand import AbsCoreCommand
from ampel.cli.MaybeIntAction import MaybeIntAction
from ampel.cli.AmpelArgumentParser import AmpelArgumentParser
from ampel.model.job.JobModel import JobModel
from ampel.model.job.InputArtifact import InputArtifact
from ampel.model.job.TaskUnitModel import TaskUnitModel
from ampel.model.job.TemplateUnitModel import TemplateUnitModel
from ampel.util.pretty import out_stack, get_time_delta
from ampel.util.debug import MockPool


class JobCommand(AbsCoreCommand):
	"""
	Processes ampel "jobs" (yaml files).
	A job:
	- is a sharable analysis shema
	- contains configurations of processor units to be run sequentially
	- should also contain everything required to run the underlying processors, that is:
	  channels or aliases definitions and custom resources potentially required by processors.
	- requires access to an ampel config file since the database, unit and resource definitions
	  defined therein are necessary (might be improved in the future)
	"""

	def __init__(self):
		self.parser = None
		try:
			import sys, IPython
			sys.breakpointhook = IPython.embed
		except Exception:
			pass

	@staticmethod
	def get_sub_ops() -> None | list[str]:
		return None

	# Mandatory implementation
	def get_parser(self, sub_op: None | str = None) -> ArgumentParser | AmpelArgumentParser:

		if self.parser:
			return self.parser

		parser = AmpelArgumentParser('job')
		parser.args_not_required = True
		parser.set_help_descr({
			'debug': 'Debug',
			#'verbose': 'increases verbosity',
			'config': 'path to an ampel config file (yaml/json)',
			'schema': 'path to YAML job file (multiple files will be aggregated)',
			'secrets': 'path to a YAML secrets store in sops format',
			'keep-db': 'do not reset databases even if so requested by job file',
			'reset-db': 'reset databases even if not requested by job file',
			'no-agg': 'enables display of matplotlib plots via plt.show() for debugging',
			'no-breakpoint': 'ignore pdb breakpoints',
			'no-mp': 'deactivates multiprocessing by monkey patching multiprocessing.Pool with ampel.util.debug.MockPool for debugging',
			'interactive': 'you will be asked for each task whether it should be run or skipped\n' + \
				'and - if applicable - if the db should be reset. Option -task will be ignored.',
			'edit': 'edit job schema(s) before execution. Customize env variable EDITOR if need be. Default: "raw":\n' +
					'"raw": edit raw schema file(s) before parsing\n' +
					'"parsed": edit (possibly aggregated) schema after parsing by the yaml interpreter\n' +
					'"model": edit job model after templating (if applicable). Only task-related changes will be taken into account',
			'keep-edits': 'do not remove edited job schemas from temp dir',
			'fzf': 'choose schema file using fzf (linux/max only, fzf command line utility must be installed)',
			'task': 'only execute task(s) with provided index(es) [starting with 0].\n' +
					'Value "last" is supported. Ignored if -interactive is used',
			'show-plots': 'show plots created by job (requires ampel-plot-cli. Use "export AMPEL_PLOT_DPI=300" to increase png quality)',
			'show-plots-cmd': 'show command required to show plots created by job (requires ampel-plot-cli)',
			'wait-pid': 'wait until PID completition before starting the current job'
		})

		parser.req('config', type=str)
		parser.opt('schema', nargs='+')
		parser.opt('task', action=MaybeIntAction, nargs='+')
		parser.opt('interactive', action='store_true')
		parser.opt('debug', action='store_true')
		parser.opt('edit', nargs='?', default=None, const='raw')
		parser.opt('keep-edits', action='store_true')
		parser.opt('fzf', action='store_true')
		parser.opt('keep-db', action='store_true')
		parser.opt('reset-db', action='store_true')
		parser.opt('max-parallel-tasks', type=int, default=os.cpu_count())
		parser.opt('no-agg', action='store_true')
		parser.opt('no-breakpoint', action='store_true')
		parser.opt('no-mp', action='store_true')
		parser.opt('show-plots', action='store_true')
		parser.opt('show-plots-cmd', action='store_true')
		parser.opt('secrets', type=str)
		parser.opt('wait-pid', type=int, default=0)

		# Example
		parser.example('job job_file.yaml')
		parser.example('job job_part1.yaml job_part2.yaml')
		parser.example('job -keep-db -task last job_file.yaml')
		parser.example('job -show-plots job.yaml')
		parser.example('job -fzf -edit')
		return parser


	# Mandatory implementation
	def run(self, args: dict[str, Any], unknown_args: Sequence[str], sub_op: None | str = None) -> None:

		start_time = time()
		psys = platform.system().lower()
		logger = AmpelLogger.get_logger(base_flag=LogFlag.MANUAL_RUN)

		if args['no_mp']:
			"""
			# ThreadPool (dummy.Pool) is not enough in some circunstances
			# as matplotlib has issues when run outside main thread
			def Pool(**kwargs):
				kwargs['processes'] = 1
				return multiprocessing.dummy.Pool(**kwargs)
			"""
			import multiprocessing
			multiprocessing.__dict__['Pool'] = MockPool
			if not args['no_agg']:
				from ampel.util.getch import yes_no
				if yes_no('Set -no-agg option too (required for matplotlib interactions)'):
					args['no_agg'] = True

		if not args['no_agg']:
			try:
				import matplotlib as mpl
				mpl.use('Agg')
			except Exception:
				pass

		if args['no_breakpoint']:
			os.environ["PYTHONBREAKPOINT"] = "0"

		if isinstance(args['task'], (int, str)):
			args['task'] = [args['task']]

		schema_files = args['schema'] or [
			unknown_args.pop(unknown_args.index(el)) # type: ignore[attr-defined]
			for el in list(unknown_args)
			# This might conflict with very special - yet never used - config overrides
			if el.endswith('yml') or el.endswith('yaml')
		]

		if args.get('fzf') and not schema_files and psys.lower() != "windows":

			if shutil.which('fzf') is None:
				with out_stack():
					raise ValueError("Option '-fzf' requires the fzf command line utility")

			p1 = subprocess.Popen(
				['find', '.', '-name', '*.yml', '-o', '-name', '*.yaml'],
				stdout = subprocess.PIPE
			)
			p2 = subprocess.Popen('fzf', stdin=p1.stdout, stdout=subprocess.PIPE)
			out, err = p2.communicate()

			if err:
				with out_stack():
					raise ValueError("Error occured during fzf execution")

			if (selection := out.decode('utf8').strip()):
				schema_files = [selection]
			else:
				print("No schema selected")
				return

		if args.get('edit'):
			tmp_files: list[str] = []

		if args.get('edit') == 'raw':

			if schema_files:
				for sfile in list(schema_files):
					_, fname = tempfile.mkstemp(suffix='.yml', text=True)
					shutil.copyfile(sfile, fname)
					edit_job(fname)
					tmp_files.append(fname)

					if filecmp.cmp(sfile, fname):
						logger.info(f'Using original schema: {sfile}')
						os.unlink(fname)
						continue

					schema_files[schema_files.index(sfile)] = fname
					logger.info(f'Using modified schema: {fname}')
			else:
				schema_files = [
					edit_job(tempfile.mkstemp(suffix='.yml')[1])
				]

		if not schema_files:
			self.get_parser().print_help()
			return

		job, _ = self.get_job_schema(schema_files, logger, compute_sig=False)

		if args.get('edit') == 'parsed':
			fd, fname = tempfile.mkstemp(suffix='.yml')
			# Seems fd does not work with yaml.dump(), unsure why
			with open(fname, 'wt') as f:
				yaml.dump(
					ujson.loads(job.json(exclude_unset=True)), f,
					sort_keys=False, default_flow_style=None
				)
			edit_job(fname)
			tmp_files.append(fname)
			job, _ = self.get_job_schema([fname], logger, compute_sig=False)

		schema_descr = '|'.join(
			[os.path.basename(sf) for sf in schema_files]
		).replace('.yaml', '').replace('.yml', '')

		if len(schema_files) > 1:
			logger.info(f'Running job using composed schema: {schema_descr}')

		# Check or set env variable(s)
		for psys in ('any', platform.system().lower()):
			if psys in job.env:
				for k, v in job.env[psys].check.items():
					if k not in os.environ:
						logger.info(f'Environment variable {k}={v} required')
						return
					elif v and v != _maybe_int(os.environ[k]):
						logger.info(
							f'Environment variable {k} value mismatch:\n'
							f' required: {v} ({type(v)})\n'
							f' set: {os.environ[k]}'
						)
						return
				for k, v in job.env[psys].set.items():
					logger.info(f'Setting local environment variable {k}={v}')
					os.environ[k] = str(v)

		purge_db = job.mongo.reset or args['reset_db']
		if purge_db and args['keep_db']:
			logger.info('Keeping existing databases as option "-keep-db" was specified')
			purge_db = False

		if args['interactive']:

			from ampel.util.getch import yes_no

			try:
				if purge_db:
					purge_db = yes_no('Delete existing databases')

				args['task'] = []
				for i, td in enumerate(job.task):
					s = f' [{td.title}]' if td.title else ''
					if yes_no(f'Process task #{i}' + s):
						args['task'].append(i)
			except KeyboardInterrupt:
				sys.exit()

		if args['task']:

			if 'last' in args['task']:
				args['task'].remove('last')
				args['task'].append(len(job.task) - 1)

			if sum(args['task']) > 0 and not args['interactive'] and purge_db and not args['reset_db']:
				logger.info('Ampel job file requires db reset but argument "task" was provided')
				logger.info('Please add argument -reset-db to confirm you are absolutely sure...')
				return

		if job.requirements:
			dist_names = [el.lower() for el in get_dist_names()]
			missing = [dist for dist in job.requirements if dist.lower() not in dist_names]
			if missing:
				logger.info(f'Please install {try_reduce(missing)} to run this job\n')
				return # raise ValueError ?

		if not args['config'] and not os.path.exists(get_user_data_config_path()):
			from ampel.util.getch import yes_no
			if yes_no('Config seems to be missing, build and install'):
				from ampel.cli.ConfigCommand import ConfigCommand
				cc = ConfigCommand()
				cc.run({'install': True}, unknown_args=[], sub_op = 'build')

		s = f'Running job {job.name or schema_descr}'
		logger.info(s)

		print(' ' + '-'*len(s))

		# DevAmpelContext hashes automatically confid from potential IngestDirectives
		ctx = self.get_context(
			args, unknown_args, logger,
			freeze_config = False,
			ContextClass = DevAmpelContext,
			purge_db = purge_db,
			db_prefix = job.mongo.prefix,
			require_existing_db = False,
			one_db = True
		)

		config_dict = ctx.config._config
		self._patch_config(config_dict, job, logger)
		ctx.config._config = recursive_freeze(config_dict)

		# Ensure that job content saved in DB reflects options set dynamically
		if args['task']:
			for idx in sorted(args['task'], reverse=True):
				del job.task[idx]

		jtasks: list[dict[str, Any]] = []
		for i, model in enumerate(job.task):

			if isinstance(model, TemplateUnitModel):

				if model.template not in ctx.config._config['template']:
					raise ValueError(f'Unknown process template: {model.template}')

				fqn = ctx.config._config['template'][model.template]
				if ':' in fqn:
					fqn, class_name = fqn.split(':')
				else:
					class_name = fqn.split('.')[-1]
				Tpl = getattr(import_module(fqn), class_name)
				if not issubclass(Tpl, AbsProcessorTemplate):
					raise ValueError(f'Unexpected template type: {Tpl}')

				tpl = Tpl(**model.config)
				morphed_um = tpl \
					.get_model(ctx.config._config, model.dict()) \
					.dict(exclude_unset=True)

				if args.get('debug'):
					from ampel.util.pretty import prettyjson
					logger.info('Task model morphed by template:')
					for el in prettyjson(morphed_um, indent=4).split('\n'):
						logger.info(el)

				jtasks.append(morphed_um)

			else:
				jtasks.append(
					model.dict(
						exclude={'inputs', 'outputs', 'expand_with'},
						exclude_unset=True
					)
				)

			logger.info(
				f'Registering job task#{i} with ' +
				str(len(list(model.expand_with)) if model.expand_with else 1) +
				'x multiplier'
			)

		# recreate JobModel with templates resolved
		job_dict = ujson.loads(
			JobModel(
				**(
					job.dict(exclude_unset=True) | # type: ignore[arg-type]
					{
						'task': [
							td | task.dict(
								include={'inputs', 'outputs', 'expand_with', 'title'},
								exclude_unset=True
							)
							for task, td in zip(job.task, jtasks)
						]
					}
				)
			).json(exclude_unset=True)
		)

		if args.get('edit') == 'model':
			fd, fname = tempfile.mkstemp(suffix='.yml')
			# Seems fd does not work with yaml.dump(), unsure why
			with open(fname, 'wt') as f:
				yaml.dump(job_dict, f, sort_keys=False, default_flow_style=None)
			edit_job(fname)
			tmp_files.append(fname)
			with open(fname, 'rt') as f:
				job_dict = yaml.safe_load(f)
			jtasks = job_dict['task']

		if args.get('edit') and not args.get('keep_edits'):
			for el in tmp_files:
				try:
					os.unlink(el)
				except BaseException:
					pass

		if (wpid := args['wait_pid']) and psutil.pid_exists(wpid):
			logger.info(f'Waiting until process with PID {wpid} completes')
			while (psutil.pid_exists(wpid)):
				sleep(5)
			start_time = time()

		logger.info('Saving job schema')
		job_sig = build_unsafe_dict_id(job_dict, size=-64)
		ctx.db.get_collection('job').update_one(
			{'_id': job_sig},
			{'$setOnInsert': job_dict},
			upsert=True
		)

		run_ids = []
		for i, taskd in enumerate(jtasks):

			process_name = f'{job.name or schema_descr}#{i}'

			if 'title' in taskd:
				self.print_chapter(taskd['title'] if taskd.get('title') else f'Task #{i}', logger)
				#process_name += f' [{taskd['title']}]'
				del taskd['title']
			elif i != 0:
				self.print_chapter(f'Task #{i}', logger)

			if args['task'] is not None and i not in args['task']:
				logger.info(f'Skipping task #{i} as requested')
				continue

			taskd['override'] = taskd.pop('override', {}) | {'raise_exc': True}

			# Beacons have no real use in jobs (unlike prod)
			if taskd['unit'] == 'T2Worker' and 'send_beacon' not in taskd['config']:
				taskd['config']['send_beacon'] = False

			if (expand_with := job.task[i].expand_with) is not None:

				ps = []
				qs = []

				signal.signal(signal.SIGINT, signal_handler)
				signal.signal(signal.SIGTERM, signal_handler)

				try:
					for item in expand_with:

						self._fetch_inputs(job, job.task[i], item, logger)

						q: Queue = Queue()
						p = Process(
							target = run_mp_process,
							args = (
								q,
								config_dict,
								job.resolve_expressions(
									taskd,
									job.task[i],
									item
								),
								process_name,
							),
							daemon = True,
						)
						p.start()
						ps.append(p)
						qs.append(q)
					
					for i, (p, q) in enumerate(zip(ps, qs)):
						p.join()
						if (m := q.get()):
							logger.info(f'{taskd["unit"]}#{i} return value: {m}')
				except KeyboardInterrupt:
					sys.exit(1)
			
			else:
				
				self._fetch_inputs(job, job.task[i], None, logger)

				proc = ctx.loader.new_context_unit(
					model = UnitModel(**job.resolve_expressions(taskd, job.task[i])),
					context = ctx,
					process_name = process_name,
					job_sig = job_sig,
					sub_type = AbsEventUnit,
					base_log_flag = LogFlag.MANUAL_RUN
				)

				event_hdlr = EventHandler(
					proc.process_name,
					ctx.get_database(),
					raise_exc = proc.raise_exc,
					job_sig = job_sig,
					extra = {'task': i}
				)

				x = proc.run(event_hdlr)
				if event_hdlr.run_id:
					run_ids.append(event_hdlr.run_id)

				logger.info(f'{taskd["unit"]} return value: {x}')

		if len(run_ids) == 1:
			runstr = f" (run id: {run_ids[0]})"
		elif len(run_ids) > 1:
			runstr = " (run ids: " + " ".join([str(el) for el in run_ids]) + ")"
		else:
			runstr = ""

		logger.info(f'Job processed{runstr}')
		logger.info(f'Time required: {get_time_delta(start_time)}\n')

		if args.get('show_plots') or args.get('show_plots_cmd'):

			cmd = [
				'ampel', 'plot', 'show', '-stack', '300',
				'-png', os.environ.get('AMPEL_PLOT_DPI', '150'),
				'-t2', '-t3', '-base-path', 'body.plot', # to be improved later
				'-db', job.mongo.prefix, '-run-id', *[str(el) for el in run_ids],
			]

			if args.get('debug'):
				cmd.append('-debug')

			print(
				'%s\n%s command: %s' %
				('-' * 40, 'Executing' if args.get('show_plots') else 'Plot', " ".join(cmd))
			)

			if args.get('show_plots'):
				r = subprocess.run(cmd, check=True, capture_output=True, text=True)
				print(r.stdout)
				print(r.stderr)


	@staticmethod
	def print_chapter(msg: str, logger: AmpelLogger) -> None:
		logger.info(' ')
		logger.info('=' * (space := (len(msg) + 4)))
		logger.info('‖ ' + msg + ' ‖') # type: ignore
		logger.info('=' * space)
		logger.info(' ')


	@classmethod
	def get_job_schema(cls,
		schema_files: Sequence[str],
		logger: AmpelLogger,
		compute_sig: bool = True
	) -> tuple[JobModel, int]:

		lines = io.StringIO()
		for i, job_fname in enumerate(schema_files):

			if not os.path.exists(job_fname):
				with out_stack():
					raise FileNotFoundError(f'Job file not found: "{job_fname}"\n')

			with open(job_fname, 'r') as f:
				lines.write('\n'.join(f.readlines()))

		lines.seek(0)
		job = yaml.safe_load(lines)

		for k in list(job.keys()):
			# job root keys starting with % are used by own convention for yaml anchors
			# and thus need not be included in the loaded job structure
			if k.startswith('%'):
				del job[k]

		return JobModel(**job), build_unsafe_dict_id(job, size=-64) if compute_sig else 0


	@staticmethod
	def _fetch_inputs(
		job: JobModel,
		task: TaskUnitModel | TemplateUnitModel,
		item: None | str | dict | list,
		logger: AmpelLogger,
	):
		"""
		Ensure that input artifacts exist
		"""
		for artifact in task.inputs.artifacts:

			resolved_artifact = InputArtifact(
				**job.resolve_expressions(
					ujson.loads(artifact.json()), task, item
				)
			)

			if resolved_artifact.path.exists():
				logger.info(f'Artifact {resolved_artifact.name} exists at {resolved_artifact.path}')
			else:
				logger.info(
					f'Fetching artifact {resolved_artifact.name} from '
					f'{resolved_artifact.http.url} to {resolved_artifact.path}'
				)
				os.makedirs(resolved_artifact.path.parent, exist_ok=True)
				with tempfile.NamedTemporaryFile(delete=False) as tf:
					urlretrieve(resolved_artifact.http.url, tf.name)
					try:
						with tarfile.open(tf.name) as archive:
							logger.info(f'{resolved_artifact.name} is a tarball; extracting')
							os.makedirs(resolved_artifact.path)
							archive.extractall(resolved_artifact.path)
						os.unlink(tf.name)
					except tarfile.ReadError:
						os.rename(tf.name, resolved_artifact.path)


	@staticmethod
	def _patch_config(config_dict: dict[str, Any], job: JobModel, logger: AmpelLogger):

		# Add channel(s)
		for chan in job.channel:
			logger.info(f'Registering job channel "{chan.channel}"')
			dict.__setitem__(config_dict['channel'], chan.channel, chan.dict())

		# Add aliase(s)
		for k, v in job.alias.items():
			if 'alias' not in config_dict:
				dict.__setitem__(config_dict, 'alias', {})
			for kk, vv in v.items():
				logger.info(f'Registering job alias "{kk}"')
				if k not in config_dict['alias']:
					dict.__setitem__(config_dict['alias'], k, {})
				dict.__setitem__(config_dict['alias'][k], kk, vv)


def run_mp_process(
	queue: Queue,
	config: dict[str, Any],
	tast_unit_model: dict[str, Any],
	process_name: str,
	job_sig: None | int = None,
	task_nbr: None | int = None,
	log_profile: str = 'default'
) -> None:

	try:

		# Create new context with serialized config
		context = DevAmpelContext.load(config, one_db=True)

		processor = context.loader.new_context_unit(
			model = UnitModel(**tast_unit_model),
			context = context,
			sub_type = AbsEventUnit,
			log_profile = log_profile,
			process_name = process_name,
			job_sig = job_sig
		)

		queue.put(
			processor.run(
				EventHandler(
					processor.process_name,
					context.get_database(),
					raise_exc = processor.raise_exc,
					job_sig = job_sig,
					extra = {'task': task_nbr}
				)
			)
		)

	except Exception as e:
		import traceback
		se = str(e)
		queue.put(
			'\n' + '#'*len(se) + '\n' + str(e) + '\n' + '#'*len(se) + '\n' +
			''.join(traceback.format_exception(type(e), e, e.__traceback__))
		)


def signal_handler(sig, frame):
	#import traceback
	print('Interrupt detected')
	#print('Stack frames:')
	#traceback.print_stack(frame)
	raise KeyboardInterrupt()


def edit_job(fname: str) -> str:
	"""
	:raises ValueError if data edition fails
	"""
	if subprocess.call(
		os.environ.get('EDITOR', 'vi') + ' ' + fname,
		shell=True
	):
		with out_stack():
			raise ValueError("Cancellation requested during schema edition")

	return fname
