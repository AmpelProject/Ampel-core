#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/cli/AbsStockCommand.py
# License:             BSD-3-Clause
# Author:              valery brinnel <firstname.lastname@gmail.com>
# Date:                25.03.2021
# Last Modified Date:  25.03.2021
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

from typing import Any, Literal
from ampel.cli.ArgParserBuilder import ArgParserBuilder
from ampel.cli.MaybeIntAction import MaybeIntAction
from ampel.cli.LoadJSONAction import LoadJSONAction
from ampel.cli.AbsCoreCommand import AbsCoreCommand
from ampel.mongo.utils import maybe_match_array
from ampel.model.UnitModel import UnitModel
from ampel.model.time.UnixTimeModel import UnixTimeModel
from ampel.model.time.TimeStringModel import TimeStringModel
from ampel.model.time.TimeLastRunModel import TimeLastRunModel
from ampel.model.time.TimeDeltaModel import TimeDeltaModel
from ampel.model.time.TimeConstraintModel import TimeConstraintModel


class AbsStockCommand(AbsCoreCommand, abstract=True):
	"""
	Base class for commands selecting/matching stock(s)
	"""

	@staticmethod
	def get_select_args_help() -> dict[str, str]:

		return {
			# Required
			'config': 'Path to an ampel config file (yaml/json)',
			# Optional
			'secrets': 'Path to a YAML secrets store in sops format',
			'log-profile': 'One of: default, compact, headerless, verbose, debug',
			'id-mapper': 'Convert stock ids using the provided id mapper (ex: ZTFIdMapper)',
			# Selection
			'stock': 'Stock id(s) (OR matched if multi-valued)',
			'channel': 'Channel(s)',
			'created-after-ts': 'Created after unix timestamp',
			'created-after-str': 'Created after date-time iso string',
			'created-after-delta': 'Created after time delta',
			'created-after-process': 'Created after last run of process with name',
			'created-before-ts': 'Created before unix timestamp',
			'created-before-str': 'Created before date-time iso string',
			'created-before-delta': 'Created before time delta',
			'created-before-process': 'Created before last run of process with name',
			'updated-after-ts': 'Updated after unix timestamp',
			'updated-after-str': 'Updated after date-time iso string',
			'updated-after-delta': 'Updated after time delta',
			'updated-after-process': 'Updated after last run of process with name',
			'updated-before-ts': 'Updated before unix timestamp',
			'updated-before-str': 'Updated before date-time iso string',
			'updated-before-delta': 'Updated before time delta',
			'updated-before-process': 'Updated before last run of process with name',
			'custom-match': 'Custom mongodb match as JSON string (ex: {"body.aKey": {"$gt": 1}})',
		}


	def add_selection_args(self, builder: ArgParserBuilder) -> None:

		# Selection args
		builder.add_group('match', 'Stock selection arguments')
		builder.add_arg('match', "stock", action=MaybeIntAction, nargs="+")
		builder.add_x_args('match',
			{'name': 'created-before-str'}, {'name': 'created-before-ts', 'type': int},
			{'name': 'created-before-delta', 'action': LoadJSONAction},
			{'name': 'created-before-process'}
		)
		builder.add_x_args('match',
			{'name': 'created-after-str'}, {'name': 'created-after-ts', 'type': int},
			{'name': 'created-after-delta', 'action': LoadJSONAction},
			{'name': 'created-after-process'}
		)
		builder.add_x_args('match',
			{'name': 'updated-before-str'}, {'name': 'updated-before-ts', 'type': int},
			{'name': 'updated-before-delta', 'action': LoadJSONAction},
			{'name': 'updated-before-process'}
		)
		builder.add_x_args('match',
			{'name': 'updated-after-str'}, {'name': 'updated-after-ts', 'type': int},
			{'name': 'updated-after-delta', 'action': LoadJSONAction},
			{'name': 'updated-after-process'}
		)
		builder.create_logic_args('match', "channel", "Channel")
		builder.create_logic_args('match', "with-tag", "Tag")
		builder.create_logic_args('match', "without-tag", "Tag", excl=True)
		builder.add_arg('match', "custom-match", metavar="#", action=LoadJSONAction)


	def get_tag(self, args: dict[str, Any]) -> None | dict[Literal['with', 'without'], dict]:

		tag: None | dict[Literal['with', 'without'], dict] = None
		if args.get('with_tag'):
			tag = {'with': args['with_tag']}
		if args.get('without_tag'):
			if tag is None:
				tag = {}
			tag['without'] = args['without_tag']
		return tag


	def build_select_model(self, args: dict[str, Any]) -> UnitModel:

		conf = {
			"created": self.get_time_model("created", args),
			"updated": self.get_time_model("updated", args),
			'channel': args['channel'],
			'custom': args['custom_match']
		}

		if args.get('tag'):
			conf['tag'] = self.get_tag(args)

		if (stock := args.get('stock')):
			conf['custom'] = {
				'_id': stock if isinstance(stock, (int, bytes, str))
					else maybe_match_array(stock)
			}

		return UnitModel(unit="T3StockSelector", config=conf)


	def get_time_model(self, prefix: str, args: dict[str, Any]) -> TimeConstraintModel:

		d: dict[str, Any] = {'after': None, 'before': None}

		for when in ('after', 'before'):
			if args.get(x := f"{prefix}_{when}_ts"):
				d[when] = UnixTimeModel(match_type='unix_time', value=args[x])
			elif args.get(x := f"{prefix}_{when}_str"):
				d[when] = TimeStringModel(match_type='time_string', dateTimeStr=args[x], dateTimeFormat="%Y%m%dT%H%M%S")
			elif args.get(x := f"{prefix}_{when}_delta"):
				d[when] = TimeDeltaModel(match_type='time_delta', **args[x])
			elif args.get(x := f"{prefix}_{when}_process"):
				d[when] = TimeLastRunModel(match_type='time_last_run', process_name=args[x])

		return TimeConstraintModel(**d)
