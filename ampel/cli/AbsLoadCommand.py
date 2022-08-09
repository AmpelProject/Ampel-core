#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/cli/AbsLoadCommand.py
# License:             BSD-3-Clause
# Author:              valery brinnel <firstname.lastname@gmail.com>
# Date:                25.03.2021
# Last Modified Date:  29.03.2021
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

from typing import Any
from bson.codec_options import CodecOptions
from ampel.cli.ArgParserBuilder import ArgParserBuilder
from ampel.cli.AbsCoreCommand import AbsCoreCommand
from ampel.model.UnitModel import UnitModel

# Maps args with load aliases
arg_doc_mapping = {
	'no_stock': "STOCK",
	'no_t0': "DATAPOINT",
	'no_t1': "COMPOUND",
	'no_t2': "T2DOC",
	'no_t3': "T3DOC"
}

class AbsLoadCommand(AbsCoreCommand, abstract=True):
	"""
	Base for commands that load ampel data using AbsT3Loader subclasses
	"""

	@staticmethod
	def get_load_args_help() -> dict[str, str]:
		return {
			'latest': 'Include only latest state (*)',
			'no-stock': 'Exclude stock document from view',
			'no-plots': 'Exclude plots',
			'no-logs': 'Exclude logs'
		}

	def add_load_args(self, builder: ArgParserBuilder, group_description: str) -> None:

		# Content args
		builder.add_group('content', group_description)
		builder.add_arg('content', 'latest', action='store_true')
		builder.add_arg('content', 'no-stock', action='store_true')
		for el in (0, 1, 2, 3):
			builder.add_arg('content', f'no-t{el}', action='store_true', help=f"Exclude t{el} documents from view")
		builder.add_arg('content', 'no-plots', action='store_true')
		builder.add_arg('content', 'no-logs', action='store_true', help="Exclude logs")

		# Note
		builder.add_all_note("Latest state means... [adequate description in a few words]")


	def build_load_model(self, args: dict[str, Any], codec_options: None | CodecOptions = None) -> UnitModel:
		return UnitModel(
			unit = "T3LatestStateDataLoader" if args.get("latest") else "T3SimpleDataLoader",
			config = {
				'channel': args['channel'],
				'codec_options': codec_options,
				'directives': [
					v for k, v in arg_doc_mapping.items() if not args[k]
				]
			}
		)
