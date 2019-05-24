#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/t2/T2Controller.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 25.01.2018
# Last Modified Date: 24.05.2019
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import logging, sys, multiprocessing, schedule
from time import time

from ampel.core.flags.T2RunStates import T2RunStates
from ampel.core.flags.LogRecordFlag import LogRecordFlag
from ampel.pipeline.t2.T2Executor import T2Executor
from ampel.pipeline.common.AmpelUtils import AmpelUtils
from ampel.pipeline.logging.AmpelLogger import AmpelLogger
from ampel.pipeline.db.AmpelDB import AmpelDB
from ampel.pipeline.common.Schedulable import Schedulable
from ampel.pipeline.config.AmpelConfig import AmpelConfig
from ampel.pipeline.common.AmpelUnitLoader import AmpelUnitLoader


class T2Controller(Schedulable):
	"""
	"""

	executors = {}

	def __init__(self, use_defaults=True): 
		"""
		"""

		# Parent constructor
		Schedulable.__init__(self)

		if use_defaults:

			# Add general executor
			# -> t2 unit unspecific
			# -> runs (every 10 mins) in the same process
			# -> without limitation regarding the max # of t2 docs to process
			self.schedule_executor()


	def schedule_executor(
		self, t2_units=None, run_state=T2RunStates.TO_RUN, 
		doc_limit=None, check_interval=10, log_level=logging.DEBUG
	):
		"""
		:param int check_interval: check interval in seconds. 
		If None, the created executor is not scheduled for 
		execution but it is added to self.executors
		"""

		dict_key = "%s%s" % (AmpelUtils.to_set(t2_units), T2RunStates.TO_RUN)

		if dict_key in self.executors:
			schedule.clear(dict_key)

		self.executors[dict_key] = T2Executor(
			t2_units=t2_units, 
			run_state=run_state, 
			log_level=logging.DEBUG,
			schedule_tag=dict_key
		)

		if check_interval:

			# Schedule processing of t2 docs
			self.get_scheduler().every(check_interval).seconds.do(
				self.executors[dict_key].process_docs, 
				extra_log_flags=LogRecordFlag.SCHEDULED_RUN,
				doc_limit=doc_limit
			).tag(dict_key)


	def schedule_mp_executor(
		self, t2_units=None, run_state=T2RunStates.TO_RUN, 
		doc_limit=None, check_interval=10, log_level=logging.DEBUG,
		join=True, stop_when_exhausted=False
	):
		"""
		"""

		# Schedule processing of t2 docs
		self.get_scheduler().every(check_interval).seconds.do(
			self.create_mp_process, 
			t2_units=t2_units, 
			run_state=run_state,
			doc_limit=doc_limit, 
			log_level=logging.DEBUG,
			join=join,
			stop_when_exhausted=stop_when_exhausted
		)

		print("MP executor scheduled")


	def create_mp_process(
		self, t2_units=None, run_state=T2RunStates.TO_RUN, 
		doc_limit=None, log_level=logging.DEBUG,
		join=True, stop_when_exhausted=False
	):
		"""
		"""

		q = multiprocessing.Queue()
		p = multiprocessing.Process(
			target=T2Controller.run_mp_executor, 
			args=(q, t2_units, run_state, doc_limit, log_level)
		)

		p.start()

		if join:

			p.join()

			if stop_when_exhausted and q.get() == 0:
				self._stop.set()
				return schedule.CancelJob


	@staticmethod
	def run_mp_executor(
		mp_queue, t2_units=None, 
		run_state=T2RunStates.TO_RUN, 
		doc_limit=None, log_level=logging.DEBUG, 
		
	):
		"""
		"""

		t2_exec = T2Executor(
			t2_units=t2_units, 
			run_state=run_state, 
			log_level=logging.DEBUG
		)

		mp_queue.put(
			t2_exec.process_docs(
				doc_limit=doc_limit,
				extra_log_flags=LogRecordFlag.SCHEDULED_RUN
			)
		)


def get_required_resources(units=None, tier=2):
	from ampel.pipeline.config.channel.ChannelConfigLoader import ChannelConfigLoader
	if units is None:
		units = set()
		for channel in ChannelConfigLoader.load_configurations(None, 2):
			for source in channel.sources:
				for t2 in source.t2Compute:
					units.add(t2.unitId)
	resources = set()
	for unit in units:
		for resource in AmpelUnitLoader.get_class(tier, unit).resources:
			resources.add(resource)
	return resources

def run():

	from ampel.pipeline.config.AmpelArgumentParser import AmpelArgumentParser
	from ampel.pipeline.config.AmpelConfig import AmpelConfig

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
		limit=opts.batch_size
	)

	if opts.interval >= 0:
		controller.run()
