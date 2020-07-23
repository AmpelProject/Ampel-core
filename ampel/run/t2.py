#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/run/t2.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 25.01.2018
# Last Modified Date: 03.04.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import logging, sys, multiprocessing
from ampel.log.AmpelLogger import AmpelLogger
from ampel.core.UnitLoader import UnitLoader
from ampel.t2.T2Controller import T2Controller


def get_required_resources(units=None, tier=2):
	from ampel.config.channel.ChannelConfigLoader import ChannelConfigLoader
	if units is None:
		units = set()
		for channel in ChannelConfigLoader.load_configurations(None, 2):
			for source in channel.sources:
				for t2 in source.t2_compute:
					units.add(t2.className)
	resources = set()
	for unit in units:
		for resource in UnitLoader.get_class(tier, unit).resources:
			resources.add(resource)
	return resources

def run():

	from ampel.run.AmpelArgumentParser import AmpelArgumentParser

	multiprocessing.log_to_stderr(logging.DEBUG)

	parser = AmpelArgumentParser()
	parser.add_argument('-v', '--verbose', default=False, action="store_true")
	parser.add_argument('--units', default=None, nargs='+', help='T2 units to run')
	parser.add_argument(
		'--interval', default=10, type=int,
		help='Seconds to wait between database polls. If < 0, exit after one poll'
	)
	parser.add_argument(
		'--batch-size', default=200, type=int,
		help='Process this many T2 docs at a time'
	)

	parser.add_argument('--raise-exc', default=False, action="store_true", help='Raise exceptions immediately instead of logging')

	parser.require_resource('mongo', ['writer', 'logger'])
	# partially parse command line to get config
	opts, argv = parser.parse_known_args(args=[])
	parser.require_resources(*get_required_resources(opts.units))
	# parse again, filling the resource config
	opts = parser.parse_args()

	AmpelLogger.set_default_stream(sys.stderr)

	controller = T2Controller(use_defaults=False)

	controller.schedule_executor(
		doc_limit=logging.DEBUG if opts.verbose else logging.INFO,
		check_interval=opts.interval,
		log_level=logging.DEBUG if opts.verbose else logging.INFO
	)

	if not opts.verbose:
		controller.executors[0].logger.quieten_console()

	controller.executors[0].process_docs(
		limit=opts.batch_size,
		raise_exc=opts.raise_exc
	)

	if opts.interval >= 0:
		controller.run()
