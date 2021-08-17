#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/cli/LogCommand.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 15.03.2021
# Last Modified Date: 23.03.2021
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from argparse import ArgumentParser
from typing import Sequence, Dict, Any, Optional, Union
from ampel.abstract.AbsIdMapper import AbsIdMapper
from ampel.base.AuxUnitRegister import AuxUnitRegister
from ampel.core.AmpelContext import AmpelContext
from ampel.cli.ArgParserBuilder import ArgParserBuilder
from ampel.cli.AbsCoreCommand import AbsCoreCommand
from ampel.cli.AmpelArgumentParser import AmpelArgumentParser
from ampel.cli.MaybeIntAction import MaybeIntAction
from ampel.cli.LoadJSONAction import LoadJSONAction
from ampel.mongo.query.var.LogsLoader import LogsLoader
from ampel.mongo.query.var.LogsMatcher import LogsMatcher
from ampel.log.LogsDumper import LogsDumper


hlp = {
	'show': 'Print logs to stdout',
	'save': 'Save logs',
	'tail': 'Like tail -f. Query interval can be customized with -refresh-rate (default: 1.0 second)\n',
	'config': 'Path to an ampel config file (yaml/json)',
	'secrets': 'Path to a YAML secrets store in sops format',
	'after': 'Match logs emitted after the provided date-time string [%%]',
	'before': 'Match logs emitted before the provided date-time string [%%]',
	'stock': 'Stock id(s) [o]',
	'channel': 'Channel(s) [o]',
	'run': 'Integer run id(s) [o]',
	'run-json': 'Matching dict for run id(s) |§|',
	'custom-key': 'Custom key to match (ex: "a" for alert id(s)). Parameter -custom-value must be set as well',
	'custom-value': 'Value(s) associated with -custom-key to be matched against [o]',
	'flag': 'Integer flag value. The $bitsAllSet matching operator is used.\nCan be used to filter based on "tier"',
	'date-format': 'Use custom date-time format for the logs output (ex: %%Y-%%m-%%d %%H:%%M:%%S)', # Note: % need to be escaped
	'id-mapper': 'Convert stock ids using the provided id mapper (ex: ZTFIdMapper)',
	'no-resolve-flag': 'Keep flag as int rather than performing str conversion\n' +
		'(ex: prints 1169 instead of "INFO|CORE|SCHEDULED_RUN|T0")',
	'no-resolve-stock': 'Keep stock as int when matching using id-mapper',
	'out': 'Path to file were output will be written (printed to stdout otherwise)',
	'to-json': 'Output json structure',
	'to-pretty-json': 'Output json structure with spacier formatting than the default one',
	'main-separator': 'Main separator (space by default, ex: 2021-03-16T22:11:09.000Z INFO...)',
	'extra-separator': 'Extra separator (space by default, ex: [run=1 stock=9])',
	'refresh-rate': 'Tail refresh interval in seconds (float, default: 1.0 second)',
	'verbose': 'Increases verbosity'
}

class LogCommand(AbsCoreCommand):
	"""
	TODO: Register and catch interupts in tail mode to avoid KeyboardInterrupt stack
	"""

	def __init__(self):
		self.parsers = {}

	# Mandatory implementation
	def get_parser(self, sub_op: Optional[str] = None) -> Union[ArgumentParser, AmpelArgumentParser]:

		if sub_op in self.parsers:
			return self.parsers[sub_op]

		sub_ops = ["show", "tail", "save"]
		if sub_op is None or sub_op not in sub_ops:
			return AmpelArgumentParser.build_choice_help(
				"log", sub_ops, hlp,
				description = 'Match and view, export or tail AMPEL logs'
			)

		builder = ArgParserBuilder("log")
		builder.add_parsers(sub_ops, hlp)

		builder.notation_add_note_references()
		builder.notation_add_example_references()

		# Required
		builder.add_arg('required', 'config', type=str)
		builder.add_arg('save.required', 'out', type=str)

		# Optional
		builder.add_arg('optional', 'secrets')
		builder.add_arg('optional', 'id-mapper', type=str)

		#parser.add_arg(g, 'tail', action='store', metavar='#', default=None, const=1.0, type=float, nargs='?')
		builder.add_arg('optional', 'verbose', action='count', default=0)
		builder.add_arg('optional', 'debug', action='count', default=0, help='Debug')
		builder.add_arg('tail.optional', 'refresh-rate', action='store', metavar='#', const=1.0, nargs='?', type=float, default=1.0)
		

		# Optional match criteria
		builder.add_group('match', 'Optional matching criteria [&]')
		builder.add_arg('show|save.match', 'after', type=str) # does not work with tail
		builder.add_arg('show|save.match', 'before', type=str)
		builder.add_arg('match', 'channel', action=MaybeIntAction, nargs='+')
		builder.add_arg('match', 'stock', action=MaybeIntAction, nargs='+')

		builder.add_x_args('match',
			{'name': 'run', 'action': MaybeIntAction, 'nargs': '+'},
			{'name': 'run-json', 'action': LoadJSONAction, 'metavar': '#', 'dest': 'run'}
		)
		builder.add_arg('match', 'flag', type=int, nargs=1)
		builder.add_arg('match', 'custom-key', type=str)
		builder.add_arg('match', 'custom-value', action=MaybeIntAction, nargs='+')

		builder.add_group('out', 'Optional output parameters')
		builder.add_x_args('out',
			{'name': 'to-json', 'action': 'store_true'},
			{'name': 'to-pretty-json', 'action': 'store_true'}
		)

		builder.add_group('format', 'Optional global format parameters')
		builder.add_arg('format', 'date-format', type=str)
		builder.add_arg('format', 'no-resolve-flag', dest='resolve_flag', action='store_false')
		builder.set_group_defaults('format', resolve_flag=True)
		builder.add_arg('format', 'no-resolve-stock', action='store_true')

		builder.add_group('show|tail.format', 'Optional specific format parameters')
		builder.add_arg('show|tail.format', 'main-separator', type=str, default=' ', const=' ', nargs='?')
		builder.add_arg('show|tail.format', 'extra-separator', type=str, default=' ', const=' ', nargs='?')

		# Notes
		builder.hint_all_query_logic(ref="&")
		builder.hint_time_format('show|save', ref="%")
		builder.add_all_note('multi-values are OR-matched', 3, ref="o")
		builder.hint_all_config_override()

		# Examples
		for el in sub_ops:
			p = f"ampel log {el} -config ampel_conf.yaml "
			a = " -out /path/to/file.txt" if el == "save" else ""
			builder.add_example(el, "-run-json '{\"$gt\": 12}'", ref="§", prepend=p, append=a)
			builder.add_example(el, '-stock 85628462', prepend=p, append=a)
			builder.add_example(el, '-run 8 -db.prefix AmpelTest', prepend=p, append=a)
			if el != "tail":
				builder.add_example(el, '-after 2020-11-03T12:12:00', prepend=p, append=a)
			builder.add_example(el, '-stock ZTF17aaatbxz ZTF20aaquaxr -id-mapper ZTFIdMapper', prepend=p, append=a)
			builder.add_example(el, '-custom-key a -custom-value 1150106945115015007 -to-pretty-json -no-resolve-flag', prepend=p, append=a)
			builder.add_example(el, '-stock ZTF20aaquast -id-mapper ZTFIdMapper', prepend=p, append=a)

		builder.add_example('tail', '-refresh-rate 10')

		self.parsers.update(
			builder.get()
		)

		return self.parsers[sub_op]


	# Mandatory implementation
	def run(self, args: Dict[str, Any], unknown_args: Sequence[str], sub_op: Optional[str] = None) -> None:

		ctx: AmpelContext = self.get_context(args, unknown_args)
		self.flag_strings: Dict = {}

		if (args['custom_key'] and not args['custom_value']) or (args['custom_value'] and not args['custom_key']):
			raise ValueError('Both parameter "--custom-key" and "--custom-value" must be used when either one is requested')

		if args['custom_key']:
			args['custom'] = {args['custom_key']: args['custom_value']}

		if args['id_mapper']:
			args['id_mapper'] = AuxUnitRegister.get_aux_class(
				args['id_mapper'], sub_type=AbsIdMapper
			)()

		matcher = LogsMatcher.new(**args)
		loader = LogsLoader(
			**{**args, 'datetime_ouput': 'date' if args['date_format'] else 'string'}
		)

		if args['no_resolve_stock']:
			args['id_mapper'] = None

		ld = LogsDumper(**args)
		col = ctx.db.get_collection('logs')
		match = matcher.get_match_criteria()

		if sub_op == "tail":
			from bson.objectid import ObjectId
			from datetime import datetime
			import time
			match['_id'] = {'$gte': ObjectId.from_datetime(datetime.utcnow())}
			ld.datetime_key = 'date'
			loader.datetime_key = 'date'

			while True:
				log_entries = loader.fetch_logs(col, match)
				if log_entries:
					next_match = log_entries[-1]['_id']
					ld.process(log_entries) # type: ignore
					match['_id'] = {'$gt': next_match}
				else:
					time.sleep(args['refresh_rate'])

		log_entries = loader.fetch_logs(col, match)
		ld.process(log_entries) # type: ignore
