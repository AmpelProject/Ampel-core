#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/cli/BufferCommand.py
# License:             BSD-3-Clause
# Author:              valery brinnel <firstname.lastname@gmail.com>
# Date:                25.03.2021
# Last Modified Date:  27.08.2022
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

import sys
from argparse import ArgumentParser
from typing import Any
from collections.abc import Sequence
from ampel.log.LogFlag import LogFlag
from ampel.core.AmpelContext import AmpelContext
from ampel.cli.utils import maybe_load_idmapper
from ampel.cli.ArgParserBuilder import ArgParserBuilder
from ampel.cli.AbsStockCommand import AbsStockCommand
from ampel.cli.AbsLoadCommand import AbsLoadCommand
from ampel.cli.AmpelArgumentParser import AmpelArgumentParser
from ampel.t3.T3Processor import T3Processor

hlp = {
	'config': 'Path to an ampel config file (yaml/json)',
	# Optional
	'secrets': 'Path to a YAML secrets store in sops format',
	'db': 'Database prefix. If set, "-mongo.prefix" value will be ignored',
	'out': 'Path to file where serialized views will be saved (printed to stdout otherwise)',
	'log-profile': 'One of: default, compact, headerless, verbose, debug',
	'id-mapper': 'Convert stock ids using the provided id mapper (ex: ZTFIdMapper)',
	'binary': 'Store buffers as compact BSON structures',
	'pretty': 'Show buffers using spacier prettyjson',
	'getch': 'Wait for a key to be pressed in terminal before printing next buffer (not compatible with binary option)'
}

class BufferCommand(AbsStockCommand, AbsLoadCommand):


	@staticmethod
	def get_sub_ops() -> list[str]:
		return ['show', 'save']


	# Mandatory implementation
	def get_parser(self, sub_op: None | str = None) -> ArgumentParser | AmpelArgumentParser:

		if sub_op in self.parsers:
			return self.parsers[sub_op]

		sub_ops = self.get_sub_ops()
		if sub_op is None or sub_op not in sub_ops:
			return AmpelArgumentParser.build_choice_help(
				'buffer', sub_ops, hlp, description = 'Show or save ampel buffers.'
			)

		builder = ArgParserBuilder('buffer')
		hlp.update(self.get_select_args_help())
		hlp.update(self.get_load_args_help())
		builder.add_parsers(sub_ops, hlp)

		builder.req('config')
		builder.req('out', 'save', default=True)

		builder.opt('secrets', default=None)
		builder.opt('id-mapper')
		builder.opt('debug', action='store_true')
		builder.opt('log-profile', default='default')
		builder.opt('binary', 'save', action='store_true')
		builder.opt('pretty', 'show', action='store_true')
		builder.opt('getch', 'show', action='store_true')

		# Selection args
		self.add_selection_args(builder)

		# Content args
		self.add_load_args(builder, 'Views content arguments')

		# Example
		builder.example('show', '-stock 85628462 -no-t0')
		builder.example('show', '-db MyDB -stock 519059889 -no-t0')
		builder.example('save', '-stock 85628462 -no-t0 -out ZTFabcdef.json')
		
		self.parsers.update(
			builder.get()
		)

		return self.parsers[sub_op]


	# Mandatory implementation
	def run(self, args: dict[str, Any], unknown_args: Sequence[str], sub_op: None | str = None) -> None:

		ctx = self.get_context(
			args, unknown_args, ContextClass = AmpelContext,
			require_existing_db = args['db'] or True, one_db='auto'
		)

		maybe_load_idmapper(args)

		conf = {
			'fd': open(args['out'], 'wb' if args.get('binary') else 'w') \
				if sub_op == 'save' else sys.stdout,
			'id_mapper': args['id_mapper'],
			'verbose': sub_op == 'save',
			'binary': args.get('binary') or False
		}

		if args.get('pretty'):
			conf['pretty'] = True

		if not args.get('binary') and args.get('getch'):
			conf['getch'] = True

		t3p = T3Processor(
			context = ctx,
			process_name = 'BufferCommand',
			template = 'compact_t3',
			base_log_flag = LogFlag.MANUAL_RUN,
			log_profile = 'console_debug' if args.get('debug') else 'console_info',
			execute = [
				{
					'supply': {
						'unit': 'T3DefaultBufferSupplier',
						'config': {
							'select': self.build_select_model(args),
							'load': self.build_load_model(args)
						}
					},
					'stage': {
						'unit': 'T3BufferExporterStager',
						'config': conf
					}
				}
			]
		)

		t3p.run()
