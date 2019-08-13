#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/t0/DelayedT0Controller.py
# License           : BSD-3-Clause
# Author            : Jakob van Santen <jakob.van.santen@desy.de>
# Date              : 19.06.2018
# Last Modified Date: 19.06.2018
# Last Modified By  : Jakob van Santen <jakob.van.santen@desy.de>

import asyncio
import logging
import multiprocessing
from astropy import units as u
from ampel.config.AmpelConfig import AmpelConfig
from ampel.ztf.archive.ArchiveDB import ArchiveDB
from ampel.t0.AlertProcessor import AlertProcessor
from ampel.common.AmpelUnitLoader import AmpelUnitLoader
from ampel.config.channel.ChannelConfigLoader import ChannelConfigLoader
from ampel.ztf.pipeline.t0.ZISetup import ZISetup

log = logging.getLogger(__name__)

async def amerge(*generators):
	"""
	Consume results from multiple async generators as they become ready
	"""
	# sleep period sufficient to ensure ordering in the event loop
	iota = 0.001
	# serialize generator output with a shared queue
	queue = asyncio.Queue(maxsize=0)
	async def consume(gen):
		async for item in gen:
			await queue.put(item)
			await asyncio.sleep(iota)
	futures = [asyncio.ensure_future(consume(gen)) for gen in generators]
	while not (all(f.done() for f in futures) and queue.empty()):
		yield await queue.get()
		await asyncio.sleep(iota)

from typing import List
from ampel.core.abstract.AbsTargetSource import AbsTargetSource

class DelayedT0Controller:
	def __init__(self, sources : List[AbsTargetSource]):
		self.sources = sources
		self._processes = {}

	@staticmethod
	def run_alertprocessor(pos, radius, dt, channels, raise_exc=False):
		"""
		Run an AlertProcessor over alerts from the given target field
		
		:parameter pos: (ra, dec) of search field center
		:parameter radius: radius of search field
		:parameter dt: (begin,end) of time period (as astropy.time.Time)
		"""
		archive = ArchiveDB(AmpelConfig.get_config('resources.archive.reader'))
		alerts = archive.get_alerts_in_cone(
		    pos[0].to(u.deg).value, pos[1].to(u.deg).value, 
		    radius.to(u.deg).value,
		    dt[0].jd, dt[1].jd)

		ap = AlertProcessor(ZISetup(serialization=None), publish_stats=['jobs'], channels=channels)
		alert_processed = ap.iter_max
		while alert_processed == ap.iter_max:
			alert_processed = ap.run(alerts, full_console_logging=False, raise_exc=raise_exc)
			logging.getLogger().info('{} alerts from {} around {}'.format(alert_processed, radius, pos))
	
	def launch_alertprocessor(self, pos, radius, dt, channels):
		"""
		Run :py:func:`run_alertprocessor` in a forked subprocess
		"""
		proc = multiprocessing.Process(target=self.run_alertprocessor, args=(pos, radius, dt, channels))
		proc.start()
		self._processes[proc.pid] = proc
	
	async def listen(self):
		"""
		Consume targets from sources as they become ready, and fork
		an AlertProcessor for each.
		"""
		sources = (s.get_targets() for s in self.sources)
		async for target in amerge(*sources):
			try:
				pos, radius, dt, channels = target
				self.launch_alertprocessor(pos, radius, dt, channels)
			except:
				log.error("Exception while processing target {}".format(target), exc_info=1)
				raise
		
		# in the rare event that the target streams finish, wait for
		# remaining AlertProcessors to complete
		for proc in self._processes.values():
			while proc.exitcode is None:
				asyncio.sleep(1)
			proc.join()
		log.info("Finishing")
	
	def run(self):
		"""
		Process targets as they arrive
		"""
		loop = asyncio.get_event_loop()
		loop.run_until_complete(self.listen())

def get_required_resources(channels=None):
	units = set()
	for channel in ChannelConfigLoader.load_configurations(channels, 0):
		for source in channel.sources:
			units.add(source.t0Filter.unitId)
	resources = set()
	for unit in units:
		for resource in AmpelUnitLoader.get_class(0, unit).resources:
			resources.add(resource)
	return resources

def replay(opts):
	"""
	Replay alerts from a cone
	"""
	from astropy import units as u

	DelayedT0Controller.run_alertprocessor(
		(opts.ra*u.deg,opts.dec*u.deg),
		opts.radius*u.deg,
		(opts.start_time,opts.stop_time),
		opts.channels,
		opts.raise_exc)

def listen(opts):
	"""
	Listen for new targets
	"""
	from ampel.config.AmpelConfig import AmpelConfig
	import pkg_resources

	sources = []
	for resource in pkg_resources.iter_entry_points('ampel.target_sources'):
		klass = resource.resolve()
		base_config = {k: AmpelConfig.get_config('resources.{}'.format(k)) for k in klass.resources}
		run_config = {}
		sources.append(klass(base_config=base_config, run_config=run_config))

	DelayedT0Controller(sources).run()

def run():
	
	from ampel.config.AmpelArgumentParser import AmpelArgumentParser
	from ampel.config.AmpelConfig import AmpelConfig
	from astropy.time import Time
	import astropy.units as u
	import pkg_resources
	from argparse import SUPPRESS

	logging.basicConfig()

	parser = AmpelArgumentParser(add_help=False)
	parser.require_resource('mongo', ['writer', 'logger'])
	parser.require_resource('archive', ['reader'])
	parser.require_resource('graphite')

	subparsers = parser.add_subparsers(help='command help', dest='command')
	subparsers.required = True
	subparser_list = []
	def add_command(f, name=None):
		if name is None:
			name = f.__name__
		p = subparsers.add_parser(name, help=f.__doc__, add_help=False)
		p.set_defaults(func=f)
		subparser_list.append(p)
		return p

	p = add_command(listen)

	p = add_command(replay)
	p.add_argument('ra', type=float, help='ra of target field (deg)')
	p.add_argument('dec', type=float, help='dec of target field (deg)')
	p.add_argument('radius', type=float, help='radius of target field (deg)')
	p.add_argument('start_time', type=Time, help='date range to replay')
	p.add_argument('stop_time', type=Time, help='date range to replay')
	p.add_argument('channels', nargs='+', help='filters channels to apply')
	p.add_argument('--raise-exc', default=False, action='store_true', help='raise exceptions from filter units')

	# partially parse command line to get config
	opts, argv = parser.parse_known_args()
	# flesh out parser with resources required by t0 units
	AmpelConfig.set_config(opts.config)
	resources = set(get_required_resources())
	if opts.func == listen:
		for resource in pkg_resources.iter_entry_points('ampel.target_sources'):
			klass = resource.resolve()
			resources.update(klass.resources)
	parser.require_resources(*resources)

	# Now that side-effect-laden parsing is done, add help
	for p in [parser] + subparser_list:
		p.add_argument('-h', '--help', action="help", default=SUPPRESS, help="show this message and exit")
	# parse again
	opts = parser.parse_args()

	opts.func(opts)
