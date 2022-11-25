#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/cli/ConfigCommand.py
# License:             BSD-3-Clause
# Author:              valery brinnel <firstname.lastname@gmail.com>
# Date:                17.07.2021
# Last Modified Date:  20.08.2022
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

import os, subprocess, json, yaml, shutil
from io import StringIO
from pathlib import Path
from time import time
from typing import Any, TextIO, Iterable
from argparse import ArgumentParser, FileType
from collections.abc import Sequence, Mapping

from ampel.core.AmpelContext import AmpelContext
from ampel.secret.AmpelVault import AmpelVault
from ampel.secret.DictSecretProvider import DictSecretProvider
from ampel.secret.PotemkinSecretProvider import PotemkinSecretProvider
from ampel.log.AmpelLogger import AmpelLogger, DEBUG, INFO
from ampel.cli.AbsCoreCommand import AbsCoreCommand
from ampel.cli.AmpelArgumentParser import AmpelArgumentParser
from ampel.cli.ArgParserBuilder import ArgParserBuilder
from ampel.cli.config import get_user_data_config_path
from ampel.config.builder.DistConfigBuilder import DistConfigBuilder
from ampel.config.builder.DisplayOptions import DisplayOptions
from ampel.util.pretty import out_stack, prettyjson

hlp = {
	'build': 'Generates a new ampel config based on information' +
		'\n from the currently installed ampel repositories',
	'show': 'Show config / config path',
	'install': 'Sets a specified ampel config as the default one in current system (conda envs supported).\n' +
		' As a consequence, the option "-config" of other CLI operations becomes optional',
	'transform': 'Transforms specified config file using jq parameters',
	'validate': 'Validates all unit configurations defined a specified config file',
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
	'distributions': 'Ampel packages to consider. If unspecified, gather all installed distributions',
	'verbose': 'verbose',
	'ext-resource': 'path to resource config file (yaml) to be integrated into the final ampel config',
	'hide-module-not-found-errors': 'Hide corresponding exceptions stack',
	'hide-stderr': 'Hide stderr messages arising during imports (from healpix for ex.)',
	'no-provenance': 'Do not retrieve and save unit module dependencies'
}


class ConfigCommand(AbsCoreCommand):


	@staticmethod
	def get_sub_ops() -> list[str]:
		return ['build', 'show', 'install', 'transform', 'validate']


	# Implement
	def get_parser(self, sub_op: None | str = None) -> ArgumentParser | AmpelArgumentParser:

		if sub_op in self.parsers:
			return self.parsers[sub_op]

		sub_ops = self.get_sub_ops()
		if sub_op is None or sub_op not in sub_ops:
			return AmpelArgumentParser.build_choice_help(
				'config', sub_ops, hlp, description = 'Build or show ampel config.'
			)

		builder = ArgParserBuilder('config')
		ps = builder.add_parsers(sub_ops, hlp)
		ps[sub_ops.index('show')].args_not_required = True

		# Required args
		builder.xargs(
			group='required', sub_ops='build', xargs = [
				{'name': 'out', 'type': str},
				{
					'name': 'install', 'action': 'store_true',
					'help': 'Installs the generated config (conda envs are supported)'
				}
			]
		)

		# Optional args
		builder.opt('secrets', 'build', default=None)
		builder.opt('verbose', 'build|show', action='store_true')

		builder.opt('sign', 'build', type=int, default=0)
		builder.opt('ext-resource', 'build')
		builder.opt('hide-module-not-found-errors', 'build', action='store_true')
		builder.opt('hide-stderr', 'build', action='store_true')
		builder.opt('no-provenance', 'build', action='store_true')
		builder.xargs(
			group='optional', sub_ops='show', xargs = [
				dict(name='json', action='store_true'),
				dict(name='path', action='store_true')
			]
		)
		builder.opt('pretty', 'show', action='store_true')
		builder.opt('stop-on-errors', 'build', default=2, type=int)
		builder.opt('distributions', 'build|install', nargs="+", default=["pyampel-", "ampel-"])
		builder.opt('file', 'install', type=str)
		builder.opt('build', 'install', action='store_true')

		builder.opt('file', 'validate', type=FileType('r'))
		builder.opt('secrets', 'validate', type=FileType('r'))

		builder.opt('file', 'transform', type=FileType('r'))
		builder.opt('out', 'transform', type=FileType('w'))
		builder.opt('filter', 'transform')
		builder.opt('validate', 'transform', action='store_true')

		# Example
		builder.example('build', '-install')
		builder.example('build', '-out ampel_conf.yaml')
		builder.example('build', '-out ampel_conf.yaml -sign -verbose')
		builder.example('build', '-out ampel_core_conf.yaml -distributions ampel-interface ampel-core')
		builder.example('show', '')
		builder.example('show', '-path')
		builder.example('show', '-json -pretty')
		builder.example('install', '-build')
		builder.example('install', '-file ampel_conf.yml')

		self.parsers.update(
			builder.get()
		)

		return self.parsers[sub_op]


	@classmethod
	def _to_strict_json(cls, obj: Any) -> Any:
		""" Get JSON-compliant representation of obj """
		if isinstance(obj, Mapping):
			assert '__nonstring_keys' not in obj
			doc = {str(k): cls._to_strict_json(v) for k, v in obj.items()}
			nonstring_keys = {
				str(k): cls._to_strict_json(k) for k in obj.keys() if not isinstance(k, str)
			}
			if nonstring_keys:
				doc['__nonstring_keys'] = nonstring_keys
			return doc
		elif isinstance(obj, Iterable) and not isinstance(obj, str):
			return [cls._to_strict_json(v) for v in obj]
		elif isinstance(obj, int) and abs(obj) >> 53:
			# use canonical BSON representation for ints larger than the precision
			# of a double
			return {'$numberLong': str(obj)}
		else:
			return obj


	@staticmethod
	def _from_strict_json(doc):
		""" Invert to_strict_json() """
		if '$numberLong' in doc:
			return int(doc['$numberLong'])
		elif '__nonstring_keys' in doc:
			nonstring_keys = doc.pop('__nonstring_keys')
			return {nonstring_keys[k]: v for k, v in doc.items()}
		else:
			return doc


	@staticmethod
	def _load_dict(source: TextIO) -> dict[str, Any]:
		if isinstance((payload := yaml.safe_load(source)), dict):
			return payload
		else:
			raise TypeError('buf does not deserialize to a dict')


	@classmethod
	def _validate(cls, config_file: TextIO, secrets: None | TextIO = None) -> None:

		from ampel.model.ChannelModel import ChannelModel
		from ampel.model.ProcessModel import ProcessModel

		ctx = AmpelContext.load(
			cls._load_dict(config_file),
			vault=AmpelVault(providers=[(
				DictSecretProvider(cls._load_dict(secrets))
				if secrets is not None
				else PotemkinSecretProvider()
			)]),
		)

		with ctx.loader.validate_unit_models():
			for channel in ctx.config.get(
				'channel', dict[str, Any], raise_exc=True
			).values():
				ChannelModel(**{k: v for k, v in channel.items() if k not in {'template'}})
			for tier in range(3):
				for process in ctx.config.get(
					f'process.t{tier}', dict[str, Any], raise_exc=True
				).values():
					ProcessModel(**process)

	# Mandatory implementation
	def run(self, args: dict[str, Any], unknown_args: Sequence[str], sub_op: None | str = None) -> None:

		logger = AmpelLogger.get_logger(
			console={'level': DEBUG if args.get('verbose', False) else INFO}
		)

		if sub_op == 'build':

			logger.info('Building config [use -verbose for more details]')

			# Fix ArgParserBuilder/ArgumentParser later
			if not args.get('out') and not args.get('install'):
				with out_stack():
					raise ValueError('Argument "out" or "install" required\n')

			start_time = time()
			cb = DistConfigBuilder(
				options = DisplayOptions(
					verbose = args.get('verbose', False),
					hide_stderr = args.get('hide_stderr', False),
					hide_module_not_found_errors = args.get('hide_module_not_found_errors', False)
				),
				logger = logger,
			)

			cb.load_distributions(prefixes=args['distributions'])
			cb.build_config(
				stop_on_errors = args['stop_on_errors'],
				skip_default_processes=True,
				config_validator = None,
				save = args.get('out') or get_user_data_config_path(),
				ext_resource = args.get('ext_resource'),
				sign = args.get('sign', 0),
				get_unit_env = not args.get('no_provenance', False),
			)

			dm = divmod(time() - start_time, 60)
			logger.info(
				'Total time required: %s minutes %s seconds\n' %
				(round(dm[0]), round(dm[1]))
			)

			logger.flush()

		elif sub_op == 'install':

			std_conf = get_user_data_config_path()
			if args['file'] and os.path.exists(args['file']):
				shutil.copy(args['file'], std_conf)
				logger.info(f'{args["file"]} successfully set as standard config ({std_conf})')
				return

			elif args['build']:
				args['out'] = std_conf
				self.run(args, unknown_args, sub_op = 'build')
				logger.info(f'New config built and installed ({std_conf})')
				return

			else:
				raise ValueError('Please provide either "file" or "build" argument')

		elif sub_op == 'show':

			conf_path = get_user_data_config_path()
			if args['path']:
				print(conf_path)
				return
			if not os.path.exists(conf_path):
				logger.info(f'Config with path {conf_path} not found')
				return
			with open(conf_path, 'r') as f:
				if args['json']:
					if args['pretty']:
						print(prettyjson(yaml.safe_load(f.read())))
					else:
						print(yaml.safe_load(f.read()))
				else:
					for l in f.readlines():
						print(l, end='')

		elif sub_op == 'transform':

			try:
				with Path(args['filter']).open() as f:
					jq_args = [f.read()]
			except (FileNotFoundError, IsADirectoryError):
				jq_args = [args['filter']]

			# Use a custom transformation to losslessly round-trip from YAML to JSON,
			# in particular:
			# - wrap large ints to prevent truncation to double precision
			# - preserve non-string keys
			input_json = json.dumps(self._to_strict_json(yaml.safe_load(args['file'])))
			config = json.loads(
				subprocess.check_output(['jq'] + jq_args, input=input_json.encode()),
				object_hook=self._from_strict_json,
			)

			with StringIO() as output_yaml:
				yaml.dump(config, output_yaml, sort_keys=False)
				if args['validate']:
					output_yaml.seek(0)
					self._validate(output_yaml)
				output_yaml.seek(0)
				args['out'].write(output_yaml.read())

		elif sub_op == 'validate':
			self._validate(args['file'], args['secrets'])
