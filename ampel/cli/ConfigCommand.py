#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/cli/ConfigCommand.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 17.07.2021
# Last Modified Date: 12.10.2021
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from argparse import ArgumentParser
from typing import Sequence, Dict, Any, Optional, Union
from ampel.log.AmpelLogger import AmpelLogger
from ampel.cli.AbsCoreCommand import AbsCoreCommand
from ampel.cli.AmpelArgumentParser import AmpelArgumentParser
from ampel.cli.ArgParserBuilder import ArgParserBuilder
from ampel.config.builder.DistConfigBuilder import DistConfigBuilder


hlp = {
	'config': 'Path to an ampel config file (yaml/json)',
	# Optional
	'secrets': 'Path to a YAML secrets store in sops format',
	'out': 'Path to file where config will be saved (printed to stdout otherwise)',
	'sign': 'Append truncated file signature (last 6 digits) to filename',
	'stop-on-errors': 'by default, config building stops and raises an exception if an error occured.\n' +
		'- 2: stop on errors\n' +
		'- 1: ignore errors in first_pass_config only (will stop on morphing/scoping/template errors)\n' +
		'- 0: ignore all errors',
	'verbose': 'verbose',
	'ext-resource': 'path to resource config file (yaml) to be integrated into the final ampel config'
}


class ConfigCommand(AbsCoreCommand):
	"""
	"""

	def __init__(self):
		self.parsers = {}


	# Mandatory implementation
	def get_parser(self, sub_op: Optional[str] = None) -> Union[ArgumentParser, AmpelArgumentParser]:

		if sub_op in self.parsers:
			return self.parsers[sub_op]

		sub_ops = ['build', 'show']

		if sub_op is None or sub_op not in sub_ops:
			return AmpelArgumentParser.build_choice_help(
				'config', sub_ops, hlp, description = 'Build or show ampel config.'
			)

		builder = ArgParserBuilder("config")
		builder.add_parsers(sub_ops, hlp)

		# Required args
		builder.add_arg("build.required", "out")

		# Optional args
		builder.add_arg("optional", "secrets", default=None)
		builder.add_arg('optional', 'verbose', action="store_true")

		builder.add_arg("build.optional", "sign", action="store_true")
		builder.add_arg("build.optional", "ext-resource")
		builder.add_arg("show.optional", "pretty", action="store_true")
		builder.add_arg("build.optional", "stop-on-errors", default=2)

		# Example
		builder.add_example("build", "-out ampel_conf.yaml")
		builder.add_example("build", "-out ampel_conf.yaml -sign -verbose")
		builder.add_example("show", "-pretty -process -tier 0 -channel CHAN1")

		self.parsers.update(
			builder.get()
		)

		return self.parsers[sub_op]



	# Mandatory implementation
	def run(self, args: Dict[str, Any], unknown_args: Sequence[str], sub_op: Optional[str] = None) -> None:

		if sub_op == 'build':

			if not (verbose := args.get('verbose', False)):
				AmpelLogger.get_logger().info(
					"Building config, this might take a while... [use -verbose for details]"
				)

			cb = DistConfigBuilder(verbose=verbose)

			cb.load_distributions()

			cb.build_config(
				stop_on_errors = 0,
				skip_default_processes=True,
				config_validator = None,
				save = args['out'],
				ext_resource = args['ext_resource']
			)

		else:
			raise NotImplementedError("Not implemented yet")
