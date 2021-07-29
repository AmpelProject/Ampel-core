#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/cli/JobCommand.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 15.03.2021
# Last Modified Date: 15.07.2021
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import yaml, os, signal, sys
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
from ampel.cli.AmpelArgumentParser import AmpelArgumentParser


class TaskUnitModel(UnitModel):
	title: Optional[str]
	multiplier: int = 1


class TemplateUnitModel(BaseModel):
	title: Optional[str]
	template: str
	config: Dict[str, Any]
	multiplier: int = 1


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

		# Help
		parser.set_help_descr({
			"config": "path to an ampel config file (yaml/json)",
			"schema": "path to YAML job file",
			"secrets": "path to a YAML secrets store in sops format",
			#"verbose": "increases verbosity",
			"debug": "Debug"
		})

		# Required
		parser.add_arg("config", "required", type=str)
		parser.add_arg("schema", "required")
		parser.add_arg("debug", "optional", action="store_true")

		# Optional
		parser.add_arg("secrets", type=str)

		# Example
		parser.add_example("job -config ampel_conf.yaml -schema job_file.yaml")

		return parser


	# Mandatory implementation
	def run(self, args: Dict[str, Any], unknown_args: Sequence[str], sub_op: Optional[str] = None) -> None:

		logger = AmpelLogger.get_logger(base_flag=LogFlag.MANUAL_RUN)

		if not os.path.exists(args['schema']):
			logger.error(f"Job file not found: '{args['schema']}'")
			return

		tds: List[Dict[str, Any]] = []
		signal.signal(signal.SIGINT, signal_handler)
		signal.signal(signal.SIGTERM, signal_handler)

		with open(args['schema'], "r") as f:

			job = yaml.safe_load(f)
			if "name" not in job:
				raise ValueError("Job name required")

			if "requirements" in job:
				# TODO: check job repo requirements
				pass

			# DevAmpelContext hashes automatically confid from potential IngestDirectives
			ctx = self.get_context(
				args, unknown_args, logger,
				freeze_config = False,
				ContextClass = DevAmpelContext,
				purge_db = get_by_path(job, 'mongo.reset') or False,
				db_prefix = get_by_path(job, 'mongo.prefix')
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

				logger.info(f"Adding job task with {tds[-1]['multiplier']}x multiplier at position {i}")

			ctx.config._config = recursive_freeze(config_dict)

			for i, task_dict in enumerate(tds):

				if 'title' in task_dict:
					self.print_chapter(task_dict['title'] if task_dict.get('title') else f"Task #{i}", logger)
					del task_dict['title']
				elif i != 0:
					self.print_chapter(f"Task #{i}", logger)

				multiplier = task_dict.pop('multiplier')
				process_name = f"{job['name']}#Task#{i}"

				if multiplier > 1:

					ps = []
					qs = []

					for i in range(multiplier):
						q: Queue = Queue()
						p = Process(
							target = run_mp_process,
							args = (q, config_dict, task_dict, process_name)
						)
						p.deamon = True
						p.start()
						ps.append(p)
						qs.append(q)

					for i in range(multiplier):
						ps[i].join()
						if (m := qs[i].get()):
							logger.info(f"{task_dict['unit']}#{i} return value: {m}")
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

		logger.info("Job processing done")


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
		context = DevAmpelContext.load(config)

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
		queue.put(e)


def signal_handler(sig, frame):
	print('Ctrl+C pressed')
	sys.exit(0)
