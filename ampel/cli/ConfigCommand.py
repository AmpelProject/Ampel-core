#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/cli/ConfigCommand.py
# License:             BSD-3-Clause
# Author:              valery brinnel <firstname.lastname@gmail.com>
# Date:                17.07.2021
# Last Modified Date:  11.08.2022
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

import os, yaml, shutil
from time import time
from typing import Any
from appdirs import user_data_dir # type: ignore[import]
from argparse import ArgumentParser
from collections.abc import Sequence

from ampel.log.AmpelLogger import AmpelLogger, DEBUG, INFO
from ampel.cli.AbsCoreCommand import AbsCoreCommand
from ampel.cli.AmpelArgumentParser import AmpelArgumentParser
from ampel.cli.ArgParserBuilder import ArgParserBuilder
from ampel.config.builder.DistConfigBuilder import DistConfigBuilder
from ampel.config.builder.DisplayOptions import DisplayOptions
from ampel.util.pretty import out_stack, prettyjson

hlp = {
	"build": "Generates a new ampel config based on information" +
		"\n from the currently installed ampel repositories",
	"show": "Show config / config path",
	"install": "Sets a specified ampel config as the default one in current system (conda envs supported).\n" +
		" As a consequence, the option '-config' of other CLI operations becomes optional",
	'file': 'Path to an ampel config file (yaml/json)',
	# Optional
	'secrets': 'Path to a YAML secrets store in sops format',
	'json': 'Show JSON encoded config',
	'pretty': 'Show pretty JSON encoded config',
	'out': 'Path to file where config will be saved',
	'path': 'Show installed config path rather than config content',
	'sign': 'Append truncated file signature (last 6 digits) to filename',
	'stop-on-errors': 'by default, config building stops and raises an exception if an error occured.\n' +
		'- 2: stop on errors\n' +
		'- 1: ignore errors in first_pass_config only (will stop on morphing/scoping/template errors)\n' +
		'- 0: ignore all errors',
	'verbose': 'verbose',
	'ext-resource': 'path to resource config file (yaml) to be integrated into the final ampel config',
	'hide-module-not-found-errors': 'Hide corresponding exceptions stack',
	'hide-stderr': 'Hide stderr messages arising during imports (from healpix for ex.)',
	'no-provenance': 'Do not retrieve and save unit module dependencies'
}


class ConfigCommand(AbsCoreCommand):


	def __init__(self):
		self.parsers = {}


	# Mandatory implementation
	def get_parser(self, sub_op: None | str = None) -> ArgumentParser | AmpelArgumentParser:

		if sub_op in self.parsers:
			return self.parsers[sub_op]

		sub_ops = ['build', 'show', 'install']

		if sub_op is None or sub_op not in sub_ops:
			return AmpelArgumentParser.build_choice_help(
				'config', sub_ops, hlp, description = 'Build or show ampel config.'
			)

		builder = ArgParserBuilder("config")
		ps = builder.add_parsers(sub_ops, hlp)
		ps[sub_ops.index('show')].args_not_required = True

		# Required args
		builder.add_x_args(
			"build.required",
			{'name': 'out', 'type': str},
			{
				'name': 'install', 'action': "store_true",
				'help': "Installs the generated config (conda envs are supported)"
			}
		)

		# Optional args
		builder.add_arg("build.optional", "secrets", default=None)
		builder.add_arg('build|show.optional', 'verbose', action="store_true")

		builder.add_arg("build.optional", "sign", type=int, default=0)
		builder.add_arg("build.optional", "ext-resource")
		builder.add_arg("build.optional", "hide-module-not-found-errors", action="store_true")
		builder.add_arg("build.optional", "hide-stderr", action="store_true")
		builder.add_arg("build.optional", "no-provenance", action="store_true")
		builder.add_x_args(
			"show.optional",
			dict(name='json', action='store_true'),
			dict(name='path', action='store_true')
		)
		builder.add_arg("show.optional", "pretty", action="store_true")
		builder.add_arg("build.optional", "stop-on-errors", default=2)
		builder.add_arg("install.optional", "file", type=str)
		builder.add_arg("install.optional", "build", action="store_true")

		# Example
		builder.add_example("build", "-install")
		builder.add_example("build", "-out ampel_conf.yaml")
		builder.add_example("build", "-out ampel_conf.yaml -sign -verbose")
		builder.add_example("show", "")
		builder.add_example("show", "-path")
		builder.add_example("show", "-json -pretty")
		builder.add_example("install", "-build")
		builder.add_example("install", "-file ampel_conf.yml")

		self.parsers.update(
			builder.get()
		)

		return self.parsers[sub_op]



	# Mandatory implementation
	def run(self, args: dict[str, Any], unknown_args: Sequence[str], sub_op: None | str = None) -> None:

		logger = AmpelLogger.get_logger(
			console={'level': DEBUG if args.get('verbose', False) else INFO}
		)

		if sub_op == 'build':

			logger.info("Building config [use -verbose for more details]")

			# Fix ArgParserBuilder/ArgumentParser later
			if not args.get('out') and not args.get('install'):
				with out_stack():
					raise ValueError("Argument 'out' or 'install' required\n")

			start_time = time()
			cb = DistConfigBuilder(
				options = DisplayOptions(
					verbose = args.get('verbose', False),
					hide_stderr = args.get('hide_stderr', False),
					hide_module_not_found_errors = args.get('hide_module_not_found_errors', False)
				),
				logger = logger
			)

			cb.load_distributions()
			cb.build_config(
				stop_on_errors = 0,
				skip_default_processes=True,
				config_validator = None,
				save = args.get('out') or self.get_installable_config_path(),
				ext_resource = args.get('ext_resource'),
				sign = args.get('sign', 0),
				get_unit_env = not args.get('no_provenance', False),
			)

			dm = divmod(time() - start_time, 60)
			logger.info(
				"Total time required: %s minutes %s seconds\n" %
				(round(dm[0]), round(dm[1]))
			)

			logger.flush()
			return

		if sub_op == 'install':

			std_conf = self.get_installable_config_path()
			if args['file'] and os.path.exists(args['file']):
				shutil.copy(args['file'], std_conf)
				logger.info(f"{args['file']} successfully set as standard config ({std_conf})")
				return

			elif args['build']:
				args['out'] = std_conf
				self.run(args, unknown_args, sub_op = 'build')
				logger.info(f"New config built and installed ({std_conf})")
				return

			else:
				raise ValueError("Please provide either 'file' or 'build' argument")

		if sub_op == 'show':
			conf_path = self.get_installable_config_path()
			if args['path']:
				print(conf_path)
				return
			if not os.path.exists(conf_path):
				logger.info(f"Config with path {conf_path} not found")
				return
			with open(conf_path, "r") as f:
				if args['json']:
					if args['pretty']:
						print(prettyjson(yaml.safe_load(f.read())))
					else:
						print(yaml.safe_load(f.read()))
				else:
					for l in f.readlines():
						print(l, end='')
			return

		raise NotImplementedError("Not implemented yet")


	def get_installable_config_path(self) -> str:

		app_path = user_data_dir("ampel")
		if not os.path.exists(app_path):
			os.makedirs(app_path)

		app_path = os.path.join(app_path, "conf")
		if not os.path.exists(app_path):
			os.makedirs(app_path)

		env = os.environ.get('CONDA_DEFAULT_ENV')
		if env:
			app_path = os.path.join(app_path, env)
			if not os.path.exists(app_path):
				os.makedirs(app_path)

		return os.path.join(app_path, "conf.yml")
