#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/cli/EventCommand.py
# License:             BSD-3-Clause
# Author:              valery brinnel <firstname.lastname@gmail.com>
# Date:                23.12.2022
# Last Modified Date:  24.12.2022
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

import yaml
from datetime import datetime
from typing import Any, Callable
from argparse import ArgumentParser
from collections.abc import Sequence
from bson.objectid import ObjectId

from ampel.core.AmpelContext import AmpelContext
from ampel.log.AmpelLogger import AmpelLogger
from ampel.log.LogFlag import LogFlag
from ampel.cli.AbsCoreCommand import AbsCoreCommand
from ampel.cli.AmpelArgumentParser import AmpelArgumentParser
from ampel.cli.MaybeIntAction import MaybeIntAction
from ampel.cli.LoadJSONAction import LoadJSONAction
from ampel.util.pretty import prettyjson


class EventCommand(AbsCoreCommand):

	def __init__(self):
		self.parser = None

	@staticmethod
	def get_sub_ops() -> None | list[str]:
		return None

	def get_parser(self, sub_op: None | str = None) -> ArgumentParser | AmpelArgumentParser:

		if self.parser:
			return self.parser

		parser = AmpelArgumentParser('event')
		parser.args_not_required = True
		parser.set_help_descr({
			'debug': 'Debug',
			'config': 'Path to an ampel config file',
			'db': 'Database prefix. If set, "-mongo.prefix" value will be ignored',
			'jobs': 'Show only job events',
			'all': 'Show unsuccesful events as well (code != 0)',
			'resolve-jobs': 'Include resolved job schema in event documents',
			'keep-oids': 'Do not convert OIDs to datetime isoformat',
			'no-pretty': 'Do not prettify JSON',
			'yaml': 'Show YAML formatted results (potential argument value [None (default), True, False]\nwill be passed to dump parameter "default_flow_style".',
			'after': 'Match events that occured after provided date-time string [%%]',
			'before': 'Match events that occured before provided date-time string [%%]',
			'run': 'Integer run id(s) [o]',
			'run-json': 'Matching dict for run id(s) |§|',
		})

		parser.opt('config', type=str)
		parser.opt('db', default=None)
		parser.opt('debug', action='store_true')
		parser.opt('jobs', action='store_true')
		parser.opt('all', action='store_true')
		parser.opt('resolve-jobs', action='store_true')
		parser.opt('keep-oids', action='store_true')
		parser.opt('no-pretty', action='store_true')
		parser.opt('yaml', type=eval, default=-1, const='None', nargs='?')
		parser.opt('after', type=str)
		parser.opt('before', type=str)
		parser.xargs_opt(
			{'name': 'run', 'action': MaybeIntAction, 'nargs': '+'},
			{'name': 'run-json', 'action': LoadJSONAction, 'metavar': '#', 'dest': 'run'}
		)
	

		parser.hint_time_format(ref='%')
		#parser.hint_all_query_logic(ref='&')
		parser.note('multi-values are OR-matched', ref='o')
		parser.example('event -db SIM -jobs -debug')
		parser.example('event -yaml -resolve-jobs -after "2022-12-19T18:26:23+00:00"')
		parser.example('event -run-json \'{"$gt": 12}\'', ref='§')
		return parser


	def run(self, args: dict[str, Any], unknown_args: Sequence[str], sub_op: None | str = None) -> None:

		logger = AmpelLogger.get_logger(base_flag=LogFlag.MANUAL_RUN)

		ctx = self.get_context(
			args, unknown_args, ContextClass=AmpelContext,
			require_existing_db = args['db'] or True, one_db='auto'
		)

		logger = AmpelLogger.from_profile(
			ctx, 'console_debug' if args['debug'] else 'console_info',
			base_flag=LogFlag.MANUAL_RUN
		)

		col = ctx.db.get_collection('event', mode='r')
		if 'event' not in col.database.list_collection_names():
			logger.info(f"Event collection does not exist (db: {col.database._Database__name})")
			return

		if args['debug']:
			logger.debug(f"Querying {col.database._Database__name} database {col.database.client.address}")

		matchd: dict[str, Any] = {} if args['all'] else {'code': 0}

		if args['jobs']:
			matchd['jobid'] = {'$exists': True}

		if args['before']:
			self.add_time_constraint(matchd, args['before'], '$lte')

		if args['after']:
			self.add_time_constraint(matchd, args['after'], '$gte')

		if args['run']:
			if isinstance(args['run'], dict):
				matchd['run'] = args['run'] # enables -run-json '{"$gt": 1}'
			else:
				matchd['run'] = args['run'] if isinstance(args['run'], int) else {'$in': args['run']}

		sep = ','
		morphers: list[Callable[[dict[str, Any]], None]] = []

		if args['no_pretty']:
			printfunc = lambda x: print(x, end='')
		elif args['yaml'] != -1:
			printfunc = lambda x: print(yaml.dump(x, sort_keys=False, default_flow_style=args['yaml']), end='')
			sep = ''
		else:
			printfunc = lambda x: print(prettyjson(x), end='')

		if not args['keep_oids']:
			morphers.append(
				lambda x: x.__setitem__(
					'_id', x['_id'].generation_time.isoformat()
				)
			)

		if args['resolve_jobs']:
			jcol = ctx.db.get_collection('job', mode='r')
			def resolve_jobs(d): # noqa
				if jobd := next(jcol.find({'_id': d.get('jobid')}), None):
					d['job'] = jobd
					del d['jobid']

			morphers.append(resolve_jobs)

		cursor = col.find(matchd)
		if firstd := next(cursor, None):
			# Print first result without prior comma
			if morphers:
				for mph in morphers:
					mph(firstd)
				printfunc(firstd)
				for el in cursor:
					print(sep)
					for mph in morphers:
						mph(el)
					printfunc(el)
			else:
				printfunc(firstd)
				for el in cursor:
					print(sep)
					printfunc(el)

		print("")


	def add_time_constraint(self, mcrit: dict[str, Any], dt: str | datetime, op: str) -> None:
		"""
		Note: time operation is greater than / *equals*
		:param dt: either datetime object or string (datetime.fromisoformat is used)
		"""

		if isinstance(dt, datetime):
			pass
		elif isinstance(dt, str):
			dt = datetime.fromisoformat(dt)
		else:
			raise ValueError()

		if "_id" not in mcrit:
			mcrit["_id"] = {}

		mcrit["_id"][op] = ObjectId.from_datetime(dt)
