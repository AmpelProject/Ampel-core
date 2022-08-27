#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/cli/T2Command.py
# License:             BSD-3-Clause
# Author:              valery brinnel <firstname.lastname@gmail.com>
# Date:                16.03.2021
# Last Modified Date:  27.08.2022
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

from json import dumps
from datetime import datetime
from argparse import ArgumentParser
from typing import Any
from collections.abc import Sequence
from ampel.core.EventHandler import EventHandler
from ampel.core.AmpelContext import AmpelContext
from ampel.abstract.AbsIdMapper import AbsIdMapper
from ampel.log.AmpelLogger import AmpelLogger
from ampel.log.LogFlag import LogFlag
from ampel.t2.T2Utils import T2Utils
from ampel.util.pretty import prettyjson
from ampel.util.serialize import walk_and_encode
from ampel.content.T2Document import T2Document
from ampel.cli.utils import maybe_load_idmapper
from ampel.cli.AbsCoreCommand import AbsCoreCommand
from ampel.cli.ArgParserBuilder import ArgParserBuilder
from ampel.cli.MaybeIntAction import MaybeIntAction
from ampel.cli.LoadJSONAction import LoadJSONAction
from ampel.cli.AmpelArgumentParser import AmpelArgumentParser

hlp = {
	"show": "Show T2 document(s) as JSON (stdout)",
	"save": "Save T2 document(s) as JSON using the provide file path",
	"reset": "Delete body and set the run state to NEW",
	"soft-reset": "Set the run state to NEW",
	"config": "Path to an ampel config file (yaml/json)",
	"secrets": "Path to a YAML secrets store in sops format",
	'out': 'Path to file were output will be written (printed to stdout otherwise)',
	'db': 'Database prefix. If set, "-mongo.prefix" value will be ignored',
	"unit": "Unit id/name",
	"limit": "Limit number of returned documents",
	"unit-config": "Unit config (integer number). Use string 'null' to match null",
	"code": "T2 code (0: COMPLETED, -1: NEW)",
	"link": "T2 document link (hex)",
	"stock": "Stock(s) associated with t2 doc (OR matched if multi-valued)",
	'id-mapper': 'Convert stock ids using the provided id mapper (ex: ZTFIdMapper)',
	"custom-match": "Custom mongodb match as JSON string (ex: {\"body.result.mychi2\": {\"$gt\": 1.0}})",
	'no-resolve-stock': 'Keep stock as int when matching using id-mapper',
	"resolve-config": "Translate 'config' field from int back to dict",
	"human-times": "Translate timestamps to human-readable strings",
	"pretty-json": "Prettify JSON output"
}

class T2Command(AbsCoreCommand):

	@staticmethod
	def get_sub_ops() -> list[str]:
		return ['show', 'save', 'reset', 'soft-reset']

	# Mandatory implementation
	def get_parser(self, sub_op: None | str = None) -> ArgumentParser | AmpelArgumentParser:

		if sub_op in self.parsers:
			return self.parsers[sub_op]

		sub_ops = self.get_sub_ops()
		if sub_op is None or sub_op not in sub_ops:
			return AmpelArgumentParser.build_choice_help(
				't2', sub_ops, hlp,
				description = 'Allows to match and then view or reset T2 documents.\n' +
				'Reset operations are registered in the event database and a\n' +
				'dedicated journal entry is added to the affected t2 documents.'
			)

		builder = ArgParserBuilder("t2")
		builder.add_parsers(sub_ops, hlp)

		builder.notation_add_note_references()
		builder.notation_add_example_references()

		# Required
		builder.opt('config')
		builder.req('out', sub_ops='save')

		# Optional general
		builder.opt('secrets', default=None)
		builder.opt('db', default=None)
		builder.opt('id-mapper', sub_ops='show|save')
		builder.opt('dry-run', action='store_true')
		builder.opt('debug', action='count', default=0, help='Debug')
		builder.opt('limit', sub_ops='show|save', action='store_true')
		builder.opt('pretty-json', sub_ops='show|save', action='store_true')

		# Optional match criteria
		builder.add_group('match', 'Optional T2 documents matching criteria', sub_ops='all')
		builder.arg('unit', group='match', nargs='+', action=MaybeIntAction)
		builder.arg('unit-config', group='match', nargs='+', action=MaybeIntAction)
		builder.arg('code', group='match', nargs='+', action=MaybeIntAction)
		builder.arg('link', group='match', nargs='+')
		builder.arg('stock', group='match', nargs='+', action=MaybeIntAction)
		builder.logic_args('run', descr='Run id', group='match', pos=0, ref='2')
		builder.logic_args('channel', descr='Channel', group='match', ref='2')
		builder.logic_args('with-tag', descr='Tag', group='match', ref='2', json=False)
		builder.logic_args('without-tag', descr='Tag', group='match', excl=True, ref='2', json=False)
		builder.arg('custom-match', group='match', metavar='#', action=LoadJSONAction)

		builder.add_group('format', 'Optional format parameters', sub_ops='show|save')
		builder.arg('resolve-config', group='format', sub_ops='show|save', action='store_true')
		builder.arg('human-times', group='format', sub_ops='show|save', action='store_true')
		builder.arg('no-resolve-stock', group='format', sub_ops='show|save', action='store_true')

		builder.note(
			'Reset operations failing to match any t2 document will not be registered in\n' +
			'in the event database but these will sliently increase the global run counter',
			sub_ops='reset|soft-reset'
		)

		# Examples
		for el in sub_ops:
			p = f"ampel t2 {el} -config ampel_conf.yaml "
			a = " -out /path/to/file" if el == "save" else ""
			builder.example(el, '-unit T2SNCosmo -db AmpelTest', prepend=p, append=a)
			builder.example(el, '-channel MY_CHANNEL -code -1', prepend=p, append=a)
			builder.example(el, '-stock 122621027 122620210 -unit DemoTiedLightCurveT2Unit', prepend=p, append=a)

		builder.example(
			'show|save',
			'-db AmpelTest -human-times -stock ZTF20aaqubac -resolve-config -id-mapper ZTFIdMapper'
		)

		self.parsers = builder.get()
		return self.parsers[sub_op]


	# Mandatory implementation
	def run(self, args: dict[str, Any], unknown_args: Sequence[str], sub_op: None | str = None) -> None:

		if sub_op is None:
			raise ValueError("A sub-operation (show, save, reset, soft-reset) needs to be specified")

		ctx = self.get_context(
			args, unknown_args, ContextClass=AmpelContext,
			require_existing_db = args['db'] or True, one_db='auto'
		)

		logger = AmpelLogger.from_profile(
			ctx, 'console_debug' if args['debug'] else 'console_info',
			base_flag=LogFlag.MANUAL_RUN
		)

		args['config'] = args.pop('unit_config')

		t2_utils = T2Utils(logger)
		col = ctx.db.get_collection('t2', mode='r')

		maybe_load_idmapper(args)
		self.convert_logical_args('tag', args)

		# args['id_mapper'] is used for matching whereas id_mapper is potentially discarded for printing
		id_mapper = None if args['no_resolve_stock'] else args['id_mapper']
		jsondump: Any = prettyjson if args['pretty_json'] else lambda x: dumps(walk_and_encode(x)) # type: ignore

		if sub_op == 'show':

			m = t2_utils.match_t2s(**args)
			limit = args.get('limit', False)
			if args.get('dry_run'):
				count = col.count_documents(m)
				if limit:
					count = min(limit, count)
				logger.info(
					f"Query: {m}\n"
					f"Number of matched documents: {count}\n"
					f"Exiting (dry-run)"
				)
				return

			c = col.find(m).limit(limit) if limit else col.find(m)

			if args['resolve_config'] or args['human_times'] or id_mapper:
				resolve_config = args['resolve_config']
				human_times = args['human_times']
				for el in c:
					self.morph_ret(ctx, el, resolve_config, human_times, id_mapper)
					print(jsondump(el))
			else:
				print(jsondump(list(c)))

		elif sub_op == 'save':

			cc = t2_utils.get_t2s(col=col, **args)
			if args.get('dry_run'):
				logger.info("Exiting (dry-run)")
				return

			resolve_config = args['resolve_config']
			human_times = args['human_times']

			with open(args['out'], 'w') as f:

				if not cc:
					f.write("[]")
					return

				f.write(
					"[\n" + jsondump(
						self.morph_ret( # type: ignore[operator]
							ctx, cc[0], resolve_config, human_times, id_mapper # type: ignore[index]
						)
					)
				)

				for el in cc[1:]: # type: ignore[index]
					f.write(
						",\n" +
						jsondump(
							self.morph_ret(ctx, el, resolve_config, human_times, id_mapper)
						)
					)
				f.write("\n]")

		# reset or soft-reset below
		elif sub_op.endswith('reset'):

			run_id = ctx.new_run_id()
			changed = t2_utils \
				.i_know_what_i_am_doing() \
				.reset_t2s(col=col, cli=True, run_id=run_id, soft = (sub_op == 'soft-reset'), **args)

			print('Number of changed T2 docs: %i' % changed)

			if changed > 0:
				# Add new doc in the 'events' collection
				EventHandler('CLI', ctx.db).register(run_id=run_id, tier=-1)


	def morph_ret(self,
		ctx: AmpelContext, doc: T2Document,
		resolve_config: bool = False,
		human_times: bool = False,
		id_mapper: None | AbsIdMapper = None
	) -> T2Document:

		if resolve_config and doc['config']:
			doc['config'] = ctx.config._config['confid'].get(doc['config'])

		if human_times:
			for el in doc['meta']:
				if 'ts' in el:
					el['ts'] = datetime.utcfromtimestamp(el['ts']).isoformat() # type: ignore

		if id_mapper:
			doc['stock'] = id_mapper.to_ext_id(doc['stock']) # type: ignore[arg-type]

		return doc
