#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/t0/DelayedT0Controller.py
# License           : BSD-3-Clause
# Author            : Jakob van Santen <jakob.van.santen@desy.de>
# Date              : 19.06.2018
# Last Modified Date: 19.06.2018
# Last Modified By  : Jakob van Santen <jakob.van.santen@desy.de>

import asyncio
import logging
import multiprocessing
from astropy import units as u
from ampel.pipeline.config.AmpelConfig import AmpelConfig
from ampel.archive import ArchiveDB
from ampel.pipeline.t0.AlertProcessor import AlertProcessor
from ampel.pipeline.t0.load.AlertSupplier import AlertSupplier
from ampel.pipeline.t0.load.ZIAlertShaper import ZIAlertShaper

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
	def run_alertprocessor(pos, radius, dt, channels):
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
		supplier = AlertSupplier(alerts, alert_shaper=ZIAlertShaper())
		
		ap = AlertProcessor(channels=channels)
		alert_processed = ap.iter_max
		while alert_processed == ap.iter_max:
			alert_processed = ap.run(supplier, console_logging=False)
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
				log.critical("Exception while processing target", exc_info=1)
		
		# in the rare event that the target streams finish, wait for
		# remaining AlertProcessors to complete
		for proc in self._processes.values():
			while proc.exitcode is None:
				asyncio.sleep(1)
			proc.join()
	
	def run(self):
		"""
		Process targets as they arrive
		"""
		loop = asyncio.get_event_loop()
		loop.run_until_complete(self.listen())

def run():
	
	from ampel.pipeline.config.AmpelArgumentParser import AmpelArgumentParser
	from ampel.pipeline.config.AmpelConfig import AmpelConfig
	from ampel.pipeline.config.ChannelLoader import ChannelLoader
	import pkg_resources

	parser = AmpelArgumentParser()
	parser.require_resource('mongo', ['writer', 'logger'])
	parser.require_resource('archive', ['reader'])
	parser.require_resource('graphite')

	# partially parse command line to get config
	opts, argv = parser.parse_known_args()
	# flesh out parser with resources required by t0 units
	loader = ChannelLoader(source="ZTFIPAC", tier=0)
	resources = set(loader.get_required_resources())
	source_classes = []
	for resource in pkg_resources.iter_entry_points('ampel.target_sources'):
		klass = resource.resolve()
		resources.update(klass.resources)
		source_classes.append(klass)
	parser.require_resources(*resources)
	# parse again
	opts = parser.parse_args()

	def create_source(klass):
		base_config = {k: AmpelConfig.get_config('resources.{}'.format(k)) for k in klass.resources}
		run_config = {}
		return klass(base_config=base_config, run_config=run_config)

	DelayedT0Controller(list(map(create_source, source_classes))).run()
