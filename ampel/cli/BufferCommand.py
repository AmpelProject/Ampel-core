#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/cli/BufferCommand.py
# License:             BSD-3-Clause
# Author:              valery brinnel <firstname.lastname@gmail.com>
# Date:                25.03.2021
# Last Modified Date:  17.07.2021
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
from ampel.model.UnitModel import UnitModel
from ampel.t3.T3Processor import T3Processor

hlp = {
	'config': 'Path to an ampel config file (yaml/json)',
	# Optional
	'secrets': 'Path to a YAML secrets store in sops format',
	'out': 'Path to file where serialized views will be saved (printed to stdout otherwise)',
	'log-profile': 'One of: default, compact, headerless, verbose, debug',
	'id-mapper': 'Convert stock ids using the provided id mapper (ex: ZTFIdMapper)',
	'binary': 'Store buffers as compact BSON structures',
	'pretty': 'Show buffers using spacier prettyjson',
	'getch': 'Wait for a key to be pressed in terminal before printing next buffer (not compatible with binary option)',
}

class BufferCommand(AbsStockCommand, AbsLoadCommand):
	"""
	TODO: implement
	"""

	def __init__(self):
		self.parsers = {}

	# Mandatory implementation
	def get_parser(self, sub_op: None | str = None) -> ArgumentParser | AmpelArgumentParser:

		if sub_op in self.parsers:
			return self.parsers[sub_op]

		sub_ops = ['show', 'save']
		if sub_op is None or sub_op not in sub_ops:
			return AmpelArgumentParser.build_choice_help(
				'buffer', sub_ops, hlp, description = 'Show or save ampel buffers.'
			)

		builder = ArgParserBuilder("buffer")
		hlp.update(self.get_select_args_help())
		hlp.update(self.get_load_args_help())
		builder.add_parsers(sub_ops, hlp)

		# Required args
		builder.add_arg("required", "config", type=str)
		builder.add_arg("save.required", "out", default=True)

		# Optional args
		builder.add_arg("optional", "secrets", default=None)
		builder.add_arg('optional', 'id-mapper')
		builder.add_arg('optional', 'debug', action="store_true")
		builder.add_arg("optional", "log-profile", default="default")
		builder.add_arg("save.optional", "binary", action="store_true")
		builder.add_arg("show.optional", "pretty", action="store_true")
		builder.add_arg("show.optional", "getch", action="store_true")

		# Selection args
		self.add_selection_args(builder)

		# Content args
		self.add_load_args(builder, 'Views content arguments')

		# Example
		builder.add_example("show", "-config ampel_conf.yaml -stock 85628462 -no-t0")
		builder.add_example("save", "-config ampel_conf.yaml -stock 85628462 -no-t0 -out ZTFabcdef.json")
		
		self.parsers.update(
			builder.get()
		)

		return self.parsers[sub_op]


	# Mandatory implementation
	def run(self, args: dict[str, Any], unknown_args: Sequence[str], sub_op: None | str = None) -> None:

		ctx: AmpelContext = self.get_context(args, unknown_args)
		maybe_load_idmapper(args)

		conf = {
			'fd': open(args["out"], "wb" if args.get('binary') else 'w') \
				if sub_op == "save" else sys.stdout,
			'id_mapper': args["id_mapper"],
			'verbose': sub_op == "save"
		}

		if args.get('pretty'):
			conf['pretty'] = True

		if not args.get('binary') and args.get('getch'):
			conf['getch'] = True

		t3p = T3Processor(
			context = ctx,
			process_name = "BufferCommand",
			base_log_flag = LogFlag.MANUAL_RUN,
			log_profile = 'console_debug' if args.get('debug') else 'console_info',
			update_journal = False,
			update_events = False,
			channel = args['channel'],
			select = self.build_select_model(args),
			load = self.build_load_model(args, codec_options=None),
			stage = UnitModel(
				unit="T3BufferBinaryExporter" if args.get('binary') \
					else "T3BufferTextExporter",
				config=conf
			)
		)

		t3p.run()
