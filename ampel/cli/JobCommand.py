#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/cli/JobCommand.py
# License:             BSD-3-Clause
# Author:              valery brinnel <firstname.lastname@gmail.com>
# Date:                15.03.2021
# Last Modified Date:  26.07.2022
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

import yaml, io, os, signal, sys, subprocess
from time import time
from multiprocessing import Queue, Process
from ampel.base.AmpelBaseModel import AmpelBaseModel
from argparse import ArgumentParser
from importlib import import_module
from typing import Any
from collections.abc import Sequence
from ampel.abstract.AbsEventUnit import AbsEventUnit
from ampel.abstract.AbsProcessorTemplate import AbsProcessorTemplate
from ampel.model.UnitModel import UnitModel
from ampel.core.EventHandler import EventHandler
from ampel.dev.DevAmpelContext import DevAmpelContext
from ampel.log.AmpelLogger import AmpelLogger
from ampel.log.LogFlag import LogFlag
from ampel.util.freeze import recursive_freeze
from ampel.util.mappings import get_by_path
from ampel.util.hash import build_unsafe_dict_id
from ampel.cli.AbsCoreCommand import AbsCoreCommand
from ampel.cli.MaybeIntAction import MaybeIntAction
from ampel.cli.AmpelArgumentParser import AmpelArgumentParser


class TaskUnitModel(UnitModel):
	title: None | str
	multiplier: int = 1


class TemplateUnitModel(AmpelBaseModel):
	title: None | str
	template: str
	config: dict[str, Any]
	multiplier: int = 1


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


	# Mandatory implementation
	def get_parser(self, sub_op: None | str = None) -> ArgumentParser | AmpelArgumentParser:

		if self.parser:
			return self.parser

		parser = AmpelArgumentParser("job")
		parser.set_help_descr({
			"debug": "Debug",
			#"verbose": "increases verbosity",
			"config": "path to an ampel config file (yaml/json)",
			"schema": "path to YAML job file (multiple files will be aggregated)",
			"secrets": "path to a YAML secrets store in sops format",
			"keep-db": "do not reset databases even if so requested by job file",
			"reset-db": "reset databases even if not requested by job file",
			"no-agg": "enables display of matplotlib plots via plt.show() for debugging",
			"interactive": "you'll be asked for each task whether it should be run or skipped\n" + \
				"and - if applicable - if the db should be reset",
			"task": "only execute task(s) with provided index(es) [starting with 0]. Value 'last' is supported",
			"show-plots": "show plots created by job (requires ampel-plot-cli)"
		})

		# Required
		parser.add_arg("config", "optional", type=str)
		parser.add_arg("schema", "required", nargs='+')
		parser.add_arg("task", "optional", action=MaybeIntAction, nargs='+')
		parser.add_arg("interactive", "optional", action="store_true")
		parser.add_arg("debug", "optional", action="store_true")
		parser.add_arg("keep-db", "optional", action="store_true")
		parser.add_arg("reset-db", "optional", action="store_true")
		parser.add_arg("no-agg", "optional", action="store_true")
		parser.add_arg("show-plots", "optional", action="store_true")

		# Optional
		parser.add_arg("secrets", type=str)

		# Example
		parser.add_example("job schema job_file.yaml")
		parser.add_example("job -schema job_file.yaml -keep-db -task last")
		parser.add_example("job -schema job_part1.yaml job_part2.yaml -show-plots (requires ampel-plot)")
		return parser


	# Mandatory implementation
	def run(self, args: dict[str, Any], unknown_args: Sequence[str], sub_op: None | str = None) -> None:

		start_time = time()
		logger = AmpelLogger.get_logger(base_flag=LogFlag.MANUAL_RUN)

		if not args['no_agg']:
			try:
				import matplotlib as mpl
				mpl.use('Agg')
			except Exception:
				pass

		if isinstance(args['task'], (int, str)):
			args['task'] = [args['task']]

		job, job_sig = self.get_job_schema(args['schema'], logger)
		schema_descr = "|".join(
			[os.path.basename(sf) for sf in args['schema']]
		).replace(".yaml", "").replace(".yml", "")
		if len(args['schema']) > 1:
			logger.info(f"Running job using composed schema: {schema_descr}")
		else:
			logger.info(f"Running job using schema {schema_descr}")

		if "requirements" in job:
			# TODO: check job repo requirements
			pass

		purge_db = get_by_path(job, 'mongo.reset') or args['reset_db']

		if purge_db and args['keep_db']:
			logger.info("Keeping existing databases ('-keep-db')")
			purge_db = False

		if args['task']:
			if 'last' in args['task']:
				args['task'].remove('last')
				args['task'].append(len(job['task']) - 1)
			if sum(args['task']) > 0 and purge_db and not args['reset_db']:
				logger.info("Ampel job file requires db reset but argument 'task' was provided")
				logger.info("Please add argument -reset-db to confirm you are absolutely sure...")
				return

		if args['interactive']:

			from ampel.util.getch import yes_no

			try:
				if purge_db:
					purge_db = yes_no("Delete existing databases")

				args['task'] = []
				for i, td in enumerate(job['task']):
					s = f" [{td['title']}]" if td['title'] else ""
					if yes_no(f"Process task #{i}" + s):
						args['task'].append(i)
			except KeyboardInterrupt:
				sys.exit()

		# DevAmpelContext hashes automatically confid from potential IngestDirectives
		ctx = self.get_context(
			args, unknown_args, logger,
			freeze_config = False,
			ContextClass = DevAmpelContext,
			purge_db = purge_db,
			db_prefix = get_by_path(job, 'mongo.prefix'),
			require_existing_db = False,
			one_db = True
		)

		config_dict = ctx.config._config

		# Add channel(s)
		for c in job.get("channel", []):
			logger.info(f"Registering job channel '{c['name']}'")
			dict.__setitem__(config_dict['channel'], c['name'], c)

		# Add aliase(s)
		for k, v in job.get("alias", {}).items():
			if k not in ("t0", "t1", "t2", "t3"):
				raise ValueError(f"Unrecognized alias: {k}")
			if 'alias' not in config_dict:
				dict.__setitem__(config_dict, 'alias', {})
			for kk, vv in v.items():
				logger.info(f"Registering job alias '{kk}'")
				if k not in config_dict['alias']:
					dict.__setitem__(config_dict['alias'], k, {})
				dict.__setitem__(config_dict['alias'][k], kk, c)

		tds: list[dict[str, Any]] = []
		for i, p in enumerate(job['task']):

			if not isinstance(p, dict):
				raise ValueError("Unsupported task definition (must be dict)")

			if 'template' in p:

				model = TemplateUnitModel(**p)
				if model.template not in ctx.config._config['template']:
					raise ValueError(f"Unknown process template: {model.template}")

				fqn = ctx.config._config['template'][model.template]
				class_name = fqn.split(".")[-1]
				Tpl = getattr(import_module(fqn), class_name)
				if not issubclass(Tpl, AbsProcessorTemplate):
					raise ValueError(f"Unexpected template type: {Tpl}")

				tpl = Tpl(**model.config)
				morphed_um = tpl \
					.get_model(ctx.config._config, model.dict()) \
					.dict() | {'title': model.title, 'multiplier': model.multiplier}

				if args.get('debug'):
					from ampel.util.pretty import prettyjson
					logger.info("Task model morphed by template:")
					for el in prettyjson(morphed_um, indent=4).split('\n'):
						logger.info(el)

				tds.append(morphed_um)

			else:
				tds.append(TaskUnitModel(**p).dict())

			logger.info(f"Registering job task#{i} with {tds[-1]['multiplier']}x multiplier")

		ctx.config._config = recursive_freeze(config_dict)
		logger.info("Saving job schema")
		ctx.db.get_collection("jobid").update_one(
			{'_id': job_sig},
			{'$setOnInsert': job},
			upsert=True
		)

		run_ids = []
		for i, task_dict in enumerate(tds):

			pname_base = job['name'] if 'name' in job else schema_descr
			process_name = f"{pname_base}#{i}"

			if 'title' in task_dict:
				self.print_chapter(task_dict['title'] if task_dict.get('title') else f"Task #{i}", logger)
				del task_dict['title']
			elif i != 0:
				self.print_chapter(f"Task #{i}", logger)

			if args['task'] is not None and i not in args['task']:
				logger.info(f"Skipping task #{i} as requested")
				continue

			multiplier = task_dict.pop('multiplier')

			# Beacons have no real use in jobs (unlike prod)
			if task_dict['unit'] == 'T2Worker' and 'send_beacon' not in task_dict['config']:
				task_dict['config']['send_beacon'] = False

			if multiplier > 1:

				ps = []
				qs = []

				signal.signal(signal.SIGINT, signal_handler)
				signal.signal(signal.SIGTERM, signal_handler)

				try:
					for _ in range(multiplier):
						q: Queue = Queue()
						p = Process(
							target = run_mp_process,
							args = (q, config_dict, task_dict, process_name, job_sig, i)
						)
						p.deamon = True
						p.start()
						ps.append(p)
						qs.append(q)

					for j in range(multiplier):
						ps[j].join()
						run_id, ps_ret = qs[j].get()
						run_ids.append(run_id)
						if ps_ret:
							logger.info(f"{task_dict['unit']}#{j} return value: {ps_ret}")
				except KeyboardInterrupt:
					sys.exit(1)

			else:

				proc = ctx.loader.new_context_unit(
					model = UnitModel(**task_dict),
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

				logger.info(f"{task_dict['unit']} return value: {x}")

		dm = divmod(time() - start_time, 60)
		logger.info(
			"Job processing done. Time required: %s minutes %s seconds\n" %
			(round(dm[0]), round(dm[1]))
		)

		if args.get("show_plots"):

			cmd = [
				'ampel', 'plot', 'show', '-stack', '100', '-png', '150',
				'-t2', '-t3', '-base-path', 'body.plot', # to be improved later
				'-job', *args['schema'], '-run-id', *[str(el) for el in run_ids]
			]
			print("-" * 40)
			print(f"Executing command: {' '.join(cmd)}")
			r = subprocess.run(cmd, check=True, capture_output=True, text=True)
			print(r.stdout)
			print(r.stderr)


	def print_chapter(self, msg: str, logger: AmpelLogger) -> None:
		logger.info(" ")
		logger.info("=" * (space := (len(msg) + 4)))
		logger.info("‖ " + msg + " ‖") # type: ignore
		logger.info("=" * space)
		logger.info(" ")


	@classmethod
	def get_job_schema(cls, schema_files: list[str], logger: AmpelLogger) -> tuple[dict, int]:

		lines = io.StringIO()
		for i, job_fname in enumerate(schema_files):

			if not os.path.exists(job_fname):
				raise ValueError(f"Job file not found: '{job_fname}'")

			with open(job_fname, "r") as f:
				lines.write("\n".join(f.readlines()))

		lines.seek(0)
		job = yaml.safe_load(lines)

		for k in list(job.keys()):
			# job keys starting with _ are used by own convention for yaml anchors
			# and thus need not be included in the loaded job structure
			if k.startswith("_"):
				del job[k]

		return job, build_unsafe_dict_id(job, size=-64)


def run_mp_process(
	queue: Queue,
	config: dict[str, Any],
	tast_unit_model: dict[str, Any],
	process_name: str,
	job_sig: None | int = None,
	task_nbr: None | int = None,
	log_profile: str = "default"
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
			"\n" + "#"*len(se) + "\n" + str(e) + "\n" + "#"*len(se) + "\n" +
			''.join(traceback.format_exception(type(e), e, e.__traceback__))
		)


def signal_handler(sig, frame):
	#import traceback
	print('Interrupt detected')
	#print("Stack frames:")
	#traceback.print_stack(frame)
	raise KeyboardInterrupt()
