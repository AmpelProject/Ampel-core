#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/cli/DBCommand.py
# License:             BSD-3-Clause
# Author:              valery brinnel <firstname.lastname@gmail.com>
# Date:                14.03.2021
# Last Modified Date:  20.08.2022
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

from typing import Any
from collections.abc import Sequence
from argparse import ArgumentParser # type: ignore[import]
from ampel.core.AmpelDB import AmpelDB
from ampel.core.AmpelContext import AmpelContext
from ampel.cli.AbsCoreCommand import AbsCoreCommand
from ampel.cli.ArgParserBuilder import ArgParserBuilder
from ampel.cli.main import AmpelArgumentParser
from ampel.log.AmpelLogger import AmpelLogger
from ampel.log.LogFlag import LogFlag


# Help parameter descriptions
hlp = {
	'import': 'Import saved ampel databases',
	'save': 'Save a databases into file',
	'delete': 'Deletes all database starting with the provided prefix (ex: AmpelTest)',
	'prefix': 'Prefix of the collections to delete (ex: AmpelTest)',
	'config': 'Path to an ampel config file (yaml/json)',
	'secrets': 'Path to a YAML secrets store in sops format',
	'log-profile': 'One of: default, compact, headerless, verbose, debug',
	'debug': 'debug',
	'force': 'Delete potententially existing view before view creation',
	'view': 'Create or discard collection views',
}

class DBCommand(AbsCoreCommand):


	@staticmethod
	def get_sub_ops() -> list[str]:
		return ['import', 'export', 'delete', 'view']


	# Mandatory implementation
	def get_parser(self, sub_op: None | str = None) -> ArgumentParser | AmpelArgumentParser:

		if sub_op in self.parsers:
			return self.parsers[sub_op]

		sub_ops = self.get_sub_ops()
		if sub_op is None or sub_op not in sub_ops:
			return AmpelArgumentParser.build_choice_help(
				'db', sub_ops, hlp, description = 'Import, export or delete ampel databases. Create or remove views.'
			)

		builder = ArgParserBuilder('db')
		builder.add_parsers(sub_ops, hlp)
		builder.notation_add_note_references()
		builder.notation_add_example_references()

		builder.req('config')
		builder.req('in', 'import')
		builder.req('out', 'export')

		builder.add_group('view', 'View arguments', sub_ops='view')
		builder.xargs(
			group='required', sub_ops='view', xargs = [
				{'name': 'create', 'action': 'store_true'},
				{'name': 'discard', 'action': 'store_true'}
			]
		)
		builder.xargs(
			group='required', sub_ops='view', xargs = [
				{'name': 'channel'},
				{'name': 'channels-or', 'nargs': '+'},
				{'name': 'channels-and', 'nargs': '+'}
			]
		)

		# Optional
		builder.opt('secrets')
		builder.opt('debug', action='store_true')
		builder.opt('force', 'view', action='store_true')

		builder.example('import', '-in /path/to/file')
		builder.example('export', '-out /path/to/file')
		builder.example('delete', '-mongo.prefix AmpelTest')
		builder.example('view', '-create -channel CHAN1')
		builder.example('view', '-create -channels-or CHAN1 CHAN2')
		builder.example('view', '-discard -channel CHAN1')

		self.parsers.update(
			builder.get()
		)

		return self.parsers[sub_op]


	# Mandatory implementation
	def run(self, args: dict[str, Any], unknown_args: Sequence[str], sub_op: None | str = None) -> None:

		if sub_op == 'delete':  # cosmetic mainly
			AmpelDB.create_collection = (lambda x: None) # type: ignore

		ctx: AmpelContext = self.get_context(args, unknown_args)
		db = ctx.db

		logger = AmpelLogger.from_profile(
			ctx, 'console_debug' if args['debug'] else 'console_info',
			base_flag = LogFlag.MANUAL_RUN
		)

		if sub_op == 'delete':

			logger.info(f'Deleting databases with prefix {ctx.db.prefix}')
			ctx.db.drop_all_databases()
			logger.info('Done')

		elif sub_op == 'view':

			if 'create' not in args and 'discard' not in args:
				logger.error('Either provide "create" or "discard" in combination with view command')
				return

			try:
				if x := args.get('channel'):
					logger.info(f'{"Creating" if args["create"] else "Removing"} view for channel {x}')
					db.create_one_view(x, logger, args['force']) if args['create'] else db.delete_one_view(x, logger)
				elif x := args.get('channels_or'):
					logger.info(f'{"Creating" if args["create"] else "Removing"} view for channels {x}')
					db.create_or_view(x, logger, args['force']) if args['create'] else db.delete_or_view(x, logger)
				elif x := args.get('channels_and'):
					logger.info(f'{"Creating" if args["create"] else "Removing"} view for channels {x}')
					db.create_and_view(x, logger, args['force']) if args['create'] else db.delete_and_view(x, logger)
				else:
					logger.error('Channel(s) required\n')
					return
			except Exception as e:
				print(e)
				if 'already exists' in str(e):
					#logger.info('View already exist')
					logger.info(str(e))
				else:
					raise e

		elif sub_op == 'export':
			raise NotImplementedError()

		elif sub_op == 'import':
			raise NotImplementedError()
