#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/common/Schedulable.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 26.05.2018
# Last Modified Date: 29.01.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import schedule, logging, threading, signal

from logging import Logger
from schedule import Scheduler
from typing import Callable, Optional
from contextlib import contextmanager

from ampel.logging.AmpelLogger import AmpelLogger


class Schedulable():
	""" """

	def __init__(self,
		start_callback: Optional[Callable] = None,
		stop_callback: Optional[Callable] = None
	) -> None:
		"""
		Optional arguments:
		:param start_callback: method to be executed before starting the run thread.
		:param stop_callback: method to be executed after run thread join().
		"""

		logging.getLogger('schedule').propagate = False
		logging.getLogger('schedule').setLevel(logging.WARNING)

		# Catch kill
		signal.signal(signal.SIGINT, self.sig_exit)
		signal.signal(signal.SIGTERM, self.sig_exit)

		# run() can be used rather that start() stop()
		self._stop = threading.Event()

		# Callback functions for start and stop methods
		self.start_callback = start_callback
		self.stop_callback = stop_callback

		# Create scheduler
		self.scheduler = schedule.Scheduler()


	def get_scheduler(self) -> Scheduler:
		"""
		Returns instance of schedule.Scheduler associated with this class instance
		"""

		return self.scheduler


	@contextmanager
	def run_in_thread(self, logger: Optional[Logger] = None):
		"""
		Executes method 'run()' in its own thread.
		If start_callback was provided as argument in constructor,
		start_callback() will be executed before run()
		"""
		self._stop.clear()

		if self.start_callback is not None:
			self.start_callback()

		self.run_thread = threading.Thread(
			target=self.run, args=(logger, )
		)

		self.run_thread.start()

		try:
			yield self

		finally:

			self._stop.set()

			if hasattr(self, 'run_thread'):
				self.run_thread.join()

			if self.stop_callback is not None:
				self.stop_callback()


	# pylint: disable=unused-argument
	def sig_exit(self, signum, frame) -> None:
		"""
		Calls method stop().
		sig_exit() is executed when SIGTERM/SIGINT is caught.
		"""
		self._stop.set()


	def run(self, logger: Logger = None) -> None:
		"""
		Runs scheduler main loop.
		See https://schedule.readthedocs.io
		"""

		if logger is None:
			logger = AmpelLogger.get_unique_logger()

		logger.info("Starting scheduler loop")

		while not self._stop.wait(1):
			self.scheduler.run_pending()

		logger.info("Stopping scheduler loop")
