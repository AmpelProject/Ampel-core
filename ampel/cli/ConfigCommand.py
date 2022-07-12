#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/cli/ConfigCommand.py
# License:             BSD-3-Clause
# Author:              valery brinnel <firstname.lastname@gmail.com>
# Date:                17.07.2021
# Last Modified Date:  12.07.2022
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

import os, shutil
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

hlp = {
	"build": "Generates a new ampel config based on information" +
		"\n from the currently installed ampel repositories",
	"show": "Not implemented yet",
	"install": "Sets a specified ampel config as the default one in current system (conda envs supported).\n" +
		" As a consequence, the option '-config' of other CLI operations becomes optional",
	'file': 'Path to an ampel config file (yaml/json)',
	# Optional
	'secrets': 'Path to a YAML secrets store in sops format',
	'out': 'Path to file where config will be saved (printed to stdout otherwise)',
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
		builder.add_parsers(sub_ops, hlp)

		# Required args
		builder.add_arg("build.required", "out")

		# Optional args
		builder.add_arg("build.optional", "secrets", default=None)
		builder.add_arg('build|show.optional', 'verbose', action="store_true")

		builder.add_arg("build.optional", "sign", type=int, default=0)
		builder.add_arg("build.optional", "ext-resource")
		builder.add_arg("build.optional", "hide-module-not-found-errors", action="store_true")
		builder.add_arg("build.optional", "hide-stderr", action="store_true")
		builder.add_arg("build.optional", "no-provenance", action="store_true")
		builder.add_arg("show.optional", "pretty", action="store_true")
		builder.add_arg("build.optional", "stop-on-errors", default=2)
		builder.add_arg("install.required", "file", type=str)

		# Example
		builder.add_example("build", "-out ampel_conf.yaml")
		builder.add_example("build", "-out ampel_conf.yaml -sign -verbose")
		builder.add_example("show", "-pretty -process -tier 0 -channel CHAN1")
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

			start_time = time()
			cb = DistConfigBuilder(
				options = DisplayOptions(
					verbose = args['verbose'],
					hide_stderr = args['hide_stderr'],
					hide_module_not_found_errors = args['hide_module_not_found_errors']
				),
				logger = logger
			)

			cb.load_distributions()
			cb.build_config(
				stop_on_errors = 0,
				skip_default_processes=True,
				config_validator = None,
				save = args['out'],
				ext_resource = args['ext_resource'],
				sign = args['sign'],
				get_unit_env = not args['no_provenance'],
			)

			dm = divmod(time() - start_time, 60)
			logger.info(
				"Total time required: %s minutes %s seconds\n" %
				(round(dm[0]), round(dm[1]))
			)

			logger.flush()
			return

		if sub_op == 'install':

			if not os.path.exists(args['file']):
				raise ValueError(f"File {args['file']} does not exist")

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

			std_conf = os.path.join(app_path, "conf.yml")
			shutil.copy(args['file'], std_conf)
			logger.info(f"{args['file']} successfully set as standard config ({std_conf})")
			return

		raise NotImplementedError("Not implemented yet")
