#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/common/Schedulable.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 26.05.2018
# Last Modified Date: 18.12.2018
# Last Modified By  : Jakob van Santen <jakob.van.santen@desy.de>

import schedule, logging, time, threading, signal, contextlib
from ampel.logging.AmpelLogger import AmpelLogger

class Schedulable():
	""" 
	"""
	logger = None
	def __init__(self, start_callback=None, stop_callback=None):
		"""
		Optional arguments:
		'start_callback': method to be executed before starting the run thread.
		'stop_callback': method to be executed after run thread join().
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

	def get_scheduler(self):
		"""
		Returns instance of schedule.Scheduler associated with this class instance
		"""

		return self.scheduler

	@contextlib.contextmanager
	def run_in_thread(self, logger=None):
		"""
		Executes method 'run()' in its own thread.
		If start_callback was provided as argument in constructor,
		start_callback() will be executed before run()
		"""
		self._stop.clear()

		if self.start_callback is not None:
			self.start_callback()

		self.run_thread = threading.Thread(target=self.run, args=(logger,))
		self.run_thread.start()

		try:
			yield self
		finally:
			self._stop.set()
			if hasattr(self, 'run_thread'):
				self.run_thread.join()

			if self.stop_callback is not None:
				self.stop_callback()

	def sig_exit(self, signum, frame):
		"""
		Calls method stop(). 
		sig_exit() is executed when SIGTERM/SIGINT is caught.
		"""
		self._stop.set()

	def run(self, logger=None):
		"""
		Runs scheduler main loop.
		See https://schedule.readthedocs.io
		"""

		# Setup logger
		if logger is None:
			if hasattr(self, 'logger') and isinstance(self.logger, logging.Logger):
				logger = self.logger
			else:
				logger = AmpelLogger.get_unique_logger()
		logger.info("Starting scheduler loop")

		while not self._stop.wait(1):
			self.scheduler.run_pending()

		logger.info("Stopping scheduler loop")
