#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/cli/AbsLoadCommand.py
# License:             BSD-3-Clause
# Author:              valery brinnel <firstname.lastname@gmail.com>
# Date:                25.03.2021
# Last Modified Date:  20.08.2022
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

from typing import Any
from ampel.cli.ArgParserBuilder import ArgParserBuilder
from ampel.cli.AbsCoreCommand import AbsCoreCommand
from ampel.model.UnitModel import UnitModel

# Maps args with load aliases
arg_doc_mapping = {
	'no_stock': "STOCK",
	'no_t0': "DATAPOINT",
	'no_t1': "COMPOUND",
	'no_t2': "T2DOC"
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

	def add_load_args(self,
		builder: ArgParserBuilder,
		group_description: str,
		sub_ops: str = 'all'
	) -> None:

		# Content args
		builder.add_group('content', group_description, sub_ops=sub_ops)
		builder.arg('latest', group='content', sub_ops=sub_ops, action='store_true')
		builder.arg('no-stock', group='content', sub_ops=sub_ops, action='store_true')
		for el in (0, 1, 2):
			builder.arg(
				f'no-t{el}', group='content', sub_ops=sub_ops,
				action='store_true', help=f"Exclude t{el} documents from view"
			)
		builder.arg('no-plots', group='content', sub_ops=sub_ops, action='store_true')
		builder.arg('no-logs', group='content', sub_ops=sub_ops, action='store_true', help="Exclude logs")

		# Note
		builder.note("Latest state means... [adequate description in a few words]")


	def build_load_model(self, args: dict[str, Any]) -> UnitModel:
		return UnitModel(
			unit = "T3LatestStateDataLoader" if args.get("latest") else "T3SimpleDataLoader",
			config = {
				'channel': args['channel'],
				'directives': [
					v for k, v in arg_doc_mapping.items() if not args[k]
				]
			}
		)
