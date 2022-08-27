#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/cli/LogCommand.py
# License:             BSD-3-Clause
# Author:              valery brinnel <firstname.lastname@gmail.com>
# Date:                15.03.2021
# Last Modified Date:  27.08.2022
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

from argparse import ArgumentParser
from typing import Any
from collections.abc import Sequence
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
	'db': 'Database prefix. If set, "-mongo.prefix" value will be ignored',
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

	@staticmethod
	def get_sub_ops() -> list[str]:
		return ['show', 'tail', 'save']

	# Mandatory implementation
	def get_parser(self, sub_op: None | str = None) -> ArgumentParser | AmpelArgumentParser:

		if sub_op in self.parsers:
			return self.parsers[sub_op]

		sub_ops = self.get_sub_ops()
		if sub_op is None or sub_op not in sub_ops:
			return AmpelArgumentParser.build_choice_help(
				'log', sub_ops, hlp,
				description = 'Match and view, export or tail AMPEL logs'
			)

		builder = ArgParserBuilder('log')
		builder.add_parsers(sub_ops, hlp)

		builder.notation_add_note_references()
		builder.notation_add_example_references()

		# Required
		builder.opt('config', type=str)
		builder.req('out', 'save', type=str)

		# Optional
		builder.opt('secrets', default=None)
		builder.opt('db', default=None)
		builder.opt('id-mapper', type=str)

		#parser.arg(g, 'tail', action='store', metavar='#', default=None, const=1.0, type=float, nargs='?')
		builder.opt('verbose', action='count', default=0)
		builder.opt('debug', action='count', default=0, help='Debug')
		builder.opt(
			'refresh-rate', 'tail', action='store', metavar='#',
			const=1.0, nargs='?', type=float, default=1.0
		)
		
		# Optional match criteria
		builder.add_group('match', 'Optional matching criteria [&]', sub_ops='all')
		builder.arg('after', group='match', sub_ops='show|save', type=str) # does not work with tail
		builder.arg('before', group='match', sub_ops='show|save', type=str)
		builder.arg('channel', group='match', action=MaybeIntAction, nargs='+')
		builder.arg('stock', group='match', action=MaybeIntAction, nargs='+')

		builder.xargs(
			group='match', sub_ops='show|save', xargs = [
				{'name': 'run', 'action': MaybeIntAction, 'nargs': '+'},
				{'name': 'run-json', 'action': LoadJSONAction, 'metavar': '#', 'dest': 'run'}
			]
		)
		builder.arg('flag', group='match', type=int, nargs=1)
		builder.arg('custom-key', group='match', type=str)
		builder.arg('custom-value', group='match', action=MaybeIntAction, nargs='+')

		builder.add_group('out', 'Optional output parameters', sub_ops='all')
		builder.xargs(
			group='out', sub_ops='all', xargs = [
				{'name': 'to-json', 'action': 'store_true'},
				{'name': 'to-pretty-json', 'action': 'store_true'}
			]
		)

		builder.add_group('format', 'Optional global format parameters', sub_ops='all')
		builder.arg('date-format', group='format', type=str)
		builder.arg('no-resolve-flag', group='format', dest='resolve_flag', action='store_false')
		builder.set_group_defaults('format', sub_ops='all', resolve_flag=True)
		builder.arg('no-resolve-stock', group='format', action='store_true')

		builder.add_group('format2', 'Optional specific format parameters', sub_ops='all')
		builder.arg(
			'main-separator', group='format2', sub_ops='all',
			type=str, default=' ', const=' ', nargs='?'
		)
		builder.arg(
			'extra-separator', group='format2', sub_ops='all',
			type=str, default=' ', const=' ', nargs='?'
		)

		# Notes
		builder.hint_all_query_logic(ref='&')
		builder.hint_time_format('show|save', ref='%')
		builder.note('multi-values are OR-matched', ref='o')
		builder.hint_all_config_override()

		# Examples
		for el in sub_ops:
			p = f'ampel log {el} '
			a = ' -out /path/to/file.txt' if el == 'save' else ''
			builder.example(el, "-run-json '{\"$gt\": 12}'", ref='§', prepend=p, append=a)
			builder.example(el, '-stock 85628462', prepend=p, append=a)
			builder.example(el, '-db MyDB -to-json', prepend=p, append=a)
			builder.example(el, '-run 8 -db AmpelTest', prepend=p, append=a)
			if el != 'tail':
				builder.example(el, '-after 2020-11-03T12:12:00', prepend=p, append=a)
			builder.example(el, '-stock ZTF17aaatbxz ZTF20aaquaxr -id-mapper ZTFIdMapper', prepend=p, append=a)
			builder.example(el, '-custom-key unit -custom-value T3DemoSavePlot -to-pretty-json -no-resolve-flag', prepend=p, append=a)
			builder.example(el, '-stock ZTF20aaquast -id-mapper ZTFIdMapper', prepend=p, append=a)

		builder.example('tail', '-refresh-rate 10')

		self.parsers.update(
			builder.get()
		)

		return self.parsers[sub_op]


	# Mandatory implementation
	def run(self, args: dict[str, Any], unknown_args: Sequence[str], sub_op: None | str = None) -> None:

		ctx = self.get_context(
			args, unknown_args, ContextClass=AmpelContext,
			require_existing_db = args['db'] or True, one_db='auto'
		)

		self.flag_strings: dict = {}

		if (
			(args['custom_key'] and not args['custom_value']) or
			(args['custom_value'] and not args['custom_key'])
		):
			raise ValueError(
				'Both parameter "--custom-key" and "--custom-value"' +
				' must be used when either one is requested'
			)

		if args['custom_key']:
			args['custom'] = {args['custom_key']: args['custom_value']}

		if args['id_mapper']:
			args['id_mapper'] = AuxUnitRegister.get_aux_class(
				args['id_mapper'], sub_type=AbsIdMapper
			)()

		matcher = LogsMatcher.new(**args)
		loader = LogsLoader(
			**(
				args | # type: ignore[arg-type]
				{'datetime_ouput': 'date' if args['date_format'] else 'string'}
			)
		)

		if args['no_resolve_stock']:
			args['id_mapper'] = None

		ld = LogsDumper(**args)
		col = ctx.db.get_collection('log')
		mcrit = matcher.get_match_criteria()

		if sub_op == 'tail':
			from bson.objectid import ObjectId
			from datetime import datetime
			import time
			mcrit['_id'] = {'$gte': ObjectId.from_datetime(datetime.utcnow())}
			ld.datetime_key = 'date'
			loader.datetime_key = 'date'

			while True:
				log_entries = loader.fetch_logs(col, mcrit)
				if log_entries:
					next_match = log_entries[-1]['_id']
					ld.process(log_entries) # type: ignore
					mcrit['_id'] = {'$gt': next_match}
				else:
					time.sleep(args['refresh_rate'])

		print(mcrit)
		log_entries = loader.fetch_logs(col, mcrit)
		ld.process(log_entries) # type: ignore
