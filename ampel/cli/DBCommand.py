#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/cli/DBCommand.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 14.03.2021
# Last Modified Date: 06.10.2021
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Optional, Sequence, Dict, Any, Union
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
	"import": "Import saved ampel databases",
	"save": "Save a databases into file",
	"delete": "Deletes all database starting with the provided prefix (ex: AmpelTest)",
	"prefix": "Prefix of the collections to delete (ex: AmpelTest)",
	"config": "Path to an ampel config file (yaml/json)",
	"secrets": "Path to a YAML secrets store in sops format",
	"log-profile": "One of: default, compact, headerless, verbose, debug",
	"debug": "debug",
	"force": "Delete potententially existing view before view creation",
}

class DBCommand(AbsCoreCommand):


	def __init__(self):
		self.parsers = {}

	# Mandatory implementation
	def get_parser(self, sub_op: Optional[str] = None) -> Union[ArgumentParser, AmpelArgumentParser]:

		if sub_op in self.parsers:
			return self.parsers[sub_op]

		sub_ops = ["import", "export", "delete", "view"]
		if sub_op is None or sub_op not in sub_ops:
			return AmpelArgumentParser.build_choice_help(
				"db", sub_ops, hlp, description = 'Import, export or delete ampel databases. Create or remove views.'
			)

		builder = ArgParserBuilder("db")
		builder.add_parsers(sub_ops, hlp)
		builder.notation_add_note_references()
		builder.notation_add_example_references()

		# Required
		builder.add_arg("required", "config")
		builder.add_arg("import.required", "in")
		builder.add_arg("export.required", "out")

		builder.add_group('view', 'View arguments')
		builder.add_x_args('view', {'name': 'create', 'action': 'store_true'}, {'name': 'discard', 'action': 'store_true'})
		builder.add_x_args(
			'view.required',
			{'name': 'channel'},
			{'name': 'channels-or', 'nargs': '+'},
			{'name': 'channels-and', 'nargs': '+'}
		)

		# Optional
		builder.add_arg("optional", "secrets")
		builder.add_arg('optional', 'debug', action="store_true")
		builder.add_arg('view.optional', 'force', action="store_true")

		builder.add_example("import", "-in /path/to/file")
		builder.add_example("export", "-out /path/to/file")
		builder.add_example("delete", "-mongo.prefix AmpelTest")
		builder.add_example("view", "-create -channel CHAN1")
		builder.add_example("view", "-create -channels-or CHAN1 CHAN2")
		builder.add_example("view", "-discard -channel CHAN1")

		self.parsers.update(
			builder.get()
		)

		return self.parsers[sub_op]


	# Mandatory implementation
	def run(self, args: Dict[str, Any], unknown_args: Sequence[str], sub_op: Optional[str] = None) -> None:

		if sub_op == "delete":  # cosmetic mainly
			AmpelDB.create_collection = (lambda x: None) # type: ignore

		ctx: AmpelContext = self.get_context(args, unknown_args)
		db = ctx.db

		logger = AmpelLogger.from_profile(
			ctx, 'console_debug' if args['debug'] else 'console_info',
			base_flag = LogFlag.MANUAL_RUN
		)

		if sub_op == "delete":

			logger.info(f"Deleting databases with prefix {ctx.db.prefix}")
			ctx.db.drop_all_databases()
			logger.info("Done")

		elif sub_op == "view":

			if 'create' not in args and 'discard' not in args:
				logger.error("Either provide 'create' or 'discard' in combination with view command")
				return

			try:
				if x := args.get("channel"):
					logger.info(f"{'Creating' if args['create'] else 'Removing'} view for channel {x}")
					db.create_one_view(x, logger, args['force']) if args['create'] else db.delete_one_view(x, logger)
				elif x := args.get("channels_or"):
					logger.info(f"{'Creating' if args['create'] else 'Removing'} view for channels {x}")
					db.create_or_view(x, logger, args['force']) if args['create'] else db.delete_or_view(x, logger)
				elif x := args.get("channels_and"):
					logger.info(f"{'Creating' if args['create'] else 'Removing'} view for channels {x}")
					db.create_and_view(x, logger, args['force']) if args['create'] else db.delete_and_view(x, logger)
				else:
					logger.error("Channel(s) required\n")
					return
			except Exception as e:
				print(e)
				if "already exists" in str(e):
					#logger.info("View already exist")
					logger.info(str(e))
				else:
					raise e

		elif sub_op == "export":
			raise NotImplementedError()

		elif sub_op == "import":
			raise NotImplementedError()
