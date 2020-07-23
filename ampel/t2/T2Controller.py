#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/t2/T2Controller.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 25.01.2018
# Last Modified Date: 30.01.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import logging, multiprocessing, schedule
from typing import ClassVar, Dict, Optional, List

from ampel.t2.T2RunState import T2RunState
from ampel.log.LogRecordFlag import LogRecordFlag
from ampel.t2.T2Processor import T2Processor
from ampel.util.collections import to_set
from ampel.core.Schedulable import Schedulable


class T2Controller(Schedulable):

	processors: ClassVar[Dict[str, T2Processor]] = {}

	def __init__(self, use_defaults=True):

		# Parent constructor
		Schedulable.__init__(self)

		if use_defaults:

			# Add general executor
			# -> t2 unit unspecific
			# -> runs (every 10 mins) in the same process
			# -> without limitation regarding the max # of t2 docs to process
			self.schedule_processor()


	def schedule_processor(self,
		t2_units: Optional[str] = None,
		run_state: T2RunState = T2RunState.TO_RUN,
		doc_limit: Optional[int] = None,
		check_interval: int = 10,
		log_level: int = logging.DEBUG
	):
		"""
		:param check_interval: check interval in seconds.
		If None, the created executor is not scheduled for
		execution but it is added to self.processors
		"""

		key = f"{to_set(t2_units)}{run_state}"

		if key in self.processors:
			schedule.clear(key)

		self.processors[key] = T2Processor(
			t2_units = t2_units,
			run_state = run_state,
			log_level = logging.DEBUG,
			schedule_tag = key
		)

		if check_interval:

			# Schedule processing of t2 docs
			self.get_scheduler() \
				.every(check_interval) \
				.seconds \
				.do(
					self.processors[key].process_t2_doc,
					extra_base_log_flags = LogRecordFlag.SCHEDULED_RUN,
					doc_limit = doc_limit
				) \
				.tag(key)


	def schedule_mp_processor(self,
		t2_units: Optional[List[str]] = None,
		run_state: T2RunState = T2RunState.TO_RUN,
		doc_limit: Optional[int] = None,
		check_interval: int = 10,
		log_level: int = logging.DEBUG,
		join: bool = True,
		stop_when_exhausted: bool = False
	) -> None:

		# Schedule processing of t2 docs
		self.get_scheduler() \
			.every(check_interval) \
			.seconds \
			.do(
				self.create_mp_process,
				t2_units = t2_units,
				run_state = run_state,
				doc_limit = doc_limit,
				log_level = logging.DEBUG,
				join = join,
				stop_when_exhausted = stop_when_exhausted
			)

		print("MP executor scheduled")


	def create_mp_process(self,
		t2_units: Optional[List[str]] = None,
		run_state: T2RunState = T2RunState.TO_RUN,
		doc_limit: Optional[int] = None,
		log_level: int = logging.DEBUG,
		join: bool = True,
		stop_when_exhausted: bool = False
	):

		q = multiprocessing.Queue()
		p = multiprocessing.Process(
			target = T2Controller.run_mp_processor,
			args = (q, t2_units, run_state, doc_limit, log_level)
		)

		p.start()

		if join:

			p.join()

			if stop_when_exhausted and q.get() == 0:
				self._stop.set()
				return schedule.CancelJob


	@staticmethod
	def run_mp_processor(
		mp_queue: multiprocessing.Queue,
		t2_units: Optional[str] = None,
		run_state: T2RunState = T2RunState.TO_RUN,
		doc_limit: Optional[int] = None,
		log_level: int = logging.DEBUG
	) -> None:

		proc = T2Processor(
			t2_units = t2_units,
			run_state = run_state,
			log_level = logging.DEBUG
		)

		mp_queue.put(
			proc.process_t2_doc(
				doc_limit = doc_limit,
				extra_base_log_flags = LogRecordFlag.SCHEDULED_RUN
			)
		)
