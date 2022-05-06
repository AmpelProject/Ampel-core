#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/cli/JobCommand.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 15.03.2021
# Last Modified Date: 17.10.2021
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import yaml, os, signal, sys
from time import time
from multiprocessing import Queue, Process
from pydantic import BaseModel
from argparse import ArgumentParser
from importlib import import_module
from typing import List, Sequence, Dict, Any, Optional, Union
from ampel.abstract.AbsEventUnit import AbsEventUnit
from ampel.abstract.AbsProcessorTemplate import AbsProcessorTemplate
from ampel.model.UnitModel import UnitModel
from ampel.dev.DevAmpelContext import DevAmpelContext
from ampel.log.AmpelLogger import AmpelLogger
from ampel.log.LogFlag import LogFlag
from ampel.util.freeze import recursive_freeze
from ampel.util.mappings import get_by_path
from ampel.cli.AbsCoreCommand import AbsCoreCommand
from ampel.cli.MaybeIntAction import MaybeIntAction
from ampel.cli.AmpelArgumentParser import AmpelArgumentParser
from ampel.model.job.JobModel import JobModel, ChannelModel, TemplateUnitModel

try:
	import matplotlib as mpl
	mpl.use('Agg')
except Exception:
	pass


class JobCommand(AbsCoreCommand):
	"""
	Processes job definitions (yaml files).
	A job:
	- can be seen as a sharable analysis shema
	- contains defintions of processor unit and config to be run sequentially
	- should contain everything that is required to run the underlying processor definitions,
	  that is: channels or aliases definitions as well as custom resources potentially required by processors.
	- requires access to an ampel config file since the database, unit or resource definitions
	  contained in the config are necessary to run the job.
	"""

	def __init__(self):
		self.parser = None

	# Mandatory implementation
	def get_parser(self, sub_op: Optional[str] = None) -> Union[ArgumentParser, AmpelArgumentParser]:

		if self.parser:
			return self.parser

		parser = AmpelArgumentParser("job")
		parser.set_help_descr({
			"debug": "Debug",
			#"verbose": "increases verbosity",
			"config": "path to an ampel config file (yaml/json)",
			"schema": "path to YAML job file",
			"secrets": "path to a YAML secrets store in sops format",
			"keep-db": "do not reset databases even if so requested by job file",
			"reset-db": "reset databases even if not requested by job file",
			"interactive": "you'll be asked for each task whether it should be run or skipped\n" + \
				"and if applicable if the db should be reset",
			"with-task": "only perform the task with provide indexes (which starts at 0)",
		})

		# Required
		parser.add_arg("config", "required", type=str)
		parser.add_arg("schema", "required")
		parser.add_arg("with-task", "optional", action=MaybeIntAction, nargs='+')
		parser.add_arg("interactive", "optional", action="store_true")
		parser.add_arg("debug", "optional", action="store_true")
		parser.add_arg("keep-db", "optional", action="store_true")
		parser.add_arg("reset-db", "optional", action="store_true")

		# Optional
		parser.add_arg("secrets", type=str)

		# Example
		parser.add_example("job -config ampel_conf.yaml schema job_file.yaml")
		parser.add_example("job -config ampel_conf.yaml -schema job_file.yaml -keep-db -with-task 2")
		return parser


	# Mandatory implementation
	def run(self, args: Dict[str, Any], unknown_args: Sequence[str], sub_op: Optional[str] = None) -> None:

		start_time = time()
		logger = AmpelLogger.get_logger(base_flag=LogFlag.MANUAL_RUN)

		if not os.path.exists(args['schema']):
			raise FileNotFoundError(f"Job file not found: '{args['schema']}'")

		tds: List[Dict[str, Any]] = []

		if isinstance(args['with_task'], int):
			args['with_task'] = [args['with_task']]

		with open(args['schema'], "r") as f:
			job = JobModel(**yaml.safe_load(f))

		s = f"Running job {job.name}"
		logger.info(s)

		print(" " + "-"*len(s))

		if job.requirements:
			# TODO: check job repo requirements
			pass

		purge_db = job.mongo.reset or args['reset_db']

		if purge_db and args['keep_db']:
			logger.info("Keeping existing databases ('-keep-db')")
			purge_db = False

		if args['with_task'] and sum(args['with_task']) > 0 and purge_db and not args['reset_db']:
			logger.info("Ampel job file requires db reset but with-task argument was provided")
			logger.info("Please add argument -reset-db to confirm you are absolutely sure...")
			return

		if args['interactive']:

			from ampel.util.getch import yes_no

			try:
				if purge_db:
					purge_db = yes_no("Delete existing databases")

				args['with_task'] = []
				for i, td in enumerate(job.task):
					s = f" [{td.title}]" if td.title else ""
					if yes_no(f"Process task #{i}" + s):
						args['with_task'].append(i)
			except KeyboardInterrupt:
				sys.exit()

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

		# Add channel(s)
		for c in job.channel:
			chan = ChannelModel(**c)
			logger.info(f"Registering job channel '{chan.channel}'")
			dict.__setitem__(config_dict['channel'], chan.channel, c)

		# Add aliase(s)
		for k, v in job.alias.items():
			if 'alias' not in config_dict:
				dict.__setitem__(config_dict, 'alias', {})
			for kk, vv in v.items():
				logger.info(f"Registering job alias '{kk}'")
				if k not in config_dict['alias']:
					dict.__setitem__(config_dict['alias'], k, {})
				dict.__setitem__(config_dict['alias'][k], kk, vv)

		for i, model in enumerate(job.task):

			if isinstance(model, TemplateUnitModel):

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
				tds.append(model.dict())

			logger.info(f"Registering job task#{i} with {tds[-1]['multiplier']}x multiplier")

		ctx.config._config = recursive_freeze(config_dict)

		for i, task_dict in enumerate(tds):

			process_name = f"{job.name}#{i}"

			if 'title' in task_dict:
				self.print_chapter(task_dict['title'] if task_dict.get('title') else f"Task #{i}", logger)
				#process_name += f" [{task_dict['title']}]"
				del task_dict['title']
			elif i != 0:
				self.print_chapter(f"Task #{i}", logger)

			if args['with_task'] is not None and i not in args['with_task']:
				logger.info(f"Skipping task #{i} as requested")
				continue

			multiplier = task_dict.pop('multiplier')
			task_dict['override'] = (task_dict.pop('override') or {}) | {'raise_exc': True}

			# Beacons have no real use in jobs (unlike prod)
			if task_dict['unit'] == 'T2Worker' and 'send_beacon' not in task_dict['config']:
				task_dict['config']['send_beacon'] = False

			if multiplier > 1:

				ps = []
				qs = []

				signal.signal(signal.SIGINT, signal_handler)
				signal.signal(signal.SIGTERM, signal_handler)

				try:
					for i in range(multiplier):
						q: Queue = Queue()
						p = Process(
							target = run_mp_process,
							args = (q, config_dict, task_dict, process_name),
							daemon = True,
						)
						p.start()
						ps.append(p)
						qs.append(q)

					for i in range(multiplier):
						ps[i].join()
						if (m := qs[i].get()):
							logger.info(f"{task_dict['unit']}#{i} return value: {m}")
				except KeyboardInterrupt:
					sys.exit(1)

			else:

				proc = ctx.loader.new_context_unit(
					model = UnitModel(**task_dict),
					context = ctx,
					process_name = process_name,
					sub_type = AbsEventUnit,
					base_log_flag = LogFlag.MANUAL_RUN
				)
				x = proc.run()
				logger.info(f"{task_dict['unit']} return value: {x}")

		dm = divmod(time() - start_time, 60)
		logger.info("Job processing done. Time required: %s minutes %s seconds\n" % (round(dm[0]), round(dm[1])))


	def print_chapter(self, msg: str, logger: AmpelLogger) -> None:
		logger.info(" ")
		logger.info("=" * (space := (len(msg) + 4)))
		logger.info("‖ " + msg + " ‖") # type: ignore
		logger.info("=" * space)
		logger.info(" ")


def run_mp_process(
	queue: Queue,
	config: Dict[str, Any],
	tast_unit_model: Dict[str, Any],
	process_name: str,
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
			process_name = process_name
		)

		queue.put(
			processor.run()
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
