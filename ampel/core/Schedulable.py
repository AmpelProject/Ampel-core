#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/core/Schedulable.py
# License:             BSD-3-Clause
# Author:              valery brinnel <firstname.lastname@gmail.com>
# Date:                26.05.2018
# Last Modified Date:  19.04.2020
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

import logging, threading, signal
from logging import Logger
from schedule import Scheduler
from collections.abc import Callable
from contextlib import contextmanager


class Schedulable:
	"""
	Class relying on module 'schedule' for scheduling jobs at given times.
	The permanent loop required by the scheduler runs within method "run_scheduler".
	The latter can be started in its own thread using start() and stopped using stop().
	The scheduler loop can also run in a context using run_in_thread() which should allow
	the python interpreter to exit after an unhandled exception.
	"""

	def __init__(self,
		start_callback: None | Callable = None,
		stop_callback: None | Callable = None,
		catch_signals: None | bool = True
	) -> None:
		"""
		:param start_callback: method to be executed before starting the run_scheduler thread.
		:param stop_callback: method to be executed after run_scheduler thread join().
		"""

		logging.getLogger('schedule').propagate = False
		logging.getLogger('schedule').setLevel(logging.WARNING)

		if catch_signals:
			# Catch term/int signals
			signal.signal(signal.SIGINT, self.sig_exit)
			signal.signal(signal.SIGTERM, self.sig_exit)

		# Callback functions for start and stop methods
		self._start_callback = start_callback
		self._stop_callback = stop_callback

		# Create scheduler
		self._scheduler = Scheduler()
		self._event = threading.Event()


	def get_scheduler(self) -> Scheduler:
		""" Returns Scheduler instance associated with this class instance """
		return self._scheduler


	@contextmanager
	def run_in_thread(self, logger: None | Logger = None):
		"""
		Runs 'run_scheduler' in its own thread using contextmanager.
		The methods start() and stop() are used under the hood.
		"""

		self.start(logger)
		try:
			yield self
		finally:
			self.stop()


	def start(self, logger: None | Logger = None) -> None:
		"""
		Executes method 'run_scheduler()' in its own thread.
		If start_callback was provided as argument in constructor,
		_start_callback() will be executed before run_scheduler()
		"""
		self._event.clear() # sets event flag to false

		if self._start_callback:
			self._start_callback()

		self._thread_run = threading.Thread(
			target=self.run_scheduler, args=(logger, )
		)

		self._thread_run.start()


	def stop(self) -> None:
		"""
		Stops run_scheduler() thread.
		If stop_callback was provided as argument in constructor,
		_stop_callback() will be executed after run_scheduler() thread joins
		"""

		self._event.set() # sets event flag to true

		if hasattr(self, '_thread_run'):
			self._thread_run.join()

		if self._stop_callback:
			self._stop_callback()


	def sig_exit(self, signum: int, frame) -> None:
		""" Executed when SIGTERM/SIGINT is caught. Stops the run_scheduler() thread. """
		self._event.set() # Sets event flag to true


	def run_scheduler(self, logger: None | Logger = None) -> None:
		"""
		Runs scheduler main loop.
		See https://schedule.readthedocs.io
		"""

		if logger:
			logger.info(f"Starting {self.__class__.__name__} scheduler loop")

		# wait() method blocks until event flag _event is true,
		# that is until _event.set() is called
		while not self._event.wait(1):
			self._scheduler.run_pending()

		if logger:
			logger.info(f"{self.__class__.__name__} scheduler loop stopped")
