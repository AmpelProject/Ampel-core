#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/common/Schedulable.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 26.05.2018
# Last Modified Date: 28.05.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import schedule, time, threading, signal
from ampel.pipeline.logging.LoggingUtils import LoggingUtils


class Schedulable():
	""" 
	"""

	def __init__(self, start_callback=None, stop_callback=None):
		"""
		Optional arguments:
		'start_callback': method to be executed before starting the run thread.
		'stop_callback': method to be executed after run thread join().
		"""

		# Catch kill
		signal.signal(signal.SIGINT, self.sig_exit)
		signal.signal(signal.SIGTERM, self.sig_exit)

		# run() can be used rather that start() stop()
		self.keep_going = True
	
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


	def start(self, logger=None):
		"""
		Executes method 'run()' in its own thread.
		If start_callback was provided as argument in constructor,
		start_callback() will be executed before run()
		"""

		# Setup logger
		self.logger = LoggingUtils.get_logger(unique=True) if logger is None else logger

		self.keep_going = True

		if self.start_callback is not None:
			self.start_callback()

		self.run_thread = threading.Thread(target=self.run, args=(logger,))
		self.run_thread.start()

	
	def stop(self):
		"""
		Stops execution of threaded method 'run()'.
		If stop_callback was provided as argument in constructor,
		stop_callback() will be executed after run thread join.
		"""

		self.keep_going = False
		self.run_thread.join()

		if self.stop_callback is not None:
			self.stop_callback()


	def sig_exit(self, signum, frame):
		"""
		Calls method stop(). 
		sig_exit() is executed when SIGTERM/SIGINT is caught.
		"""

		self.stop()


	def run(self, logger=None):
		"""
		Runs scheduler main loop.
		See https://schedule.readthedocs.io
		"""

		# Setup logger
		self.logger = LoggingUtils.get_logger(unique=True) if logger is None else logger
		self.logger.info("Starting scheduler loop")

		while self.keep_going:
			self.scheduler.run_pending()
			time.sleep(1)

		self.logger.info("Stopping scheduler loop")
