#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/t3/T3Controler.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 26.02.2018
# Last Modified Date: 06.06.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import schedule, time, threading
from ampel.pipeline.db.DBWired import DBWired
from ampel.pipeline.t3.T3Executor import T3Executor
from ampel.pipeline.t3.conf.T3Job import T3Job
from ampel.pipeline.common.Schedulable import Schedulable
from ampel.pipeline.logging.LoggingUtils import LoggingUtils

class T3Controler(DBWired, Schedulable):
	"""
	"""

	def __init__(
		self, config=None, central_db=None, mongodb_uri=None, t3_job_names=None
	):
		"""
		'config': see ampel.pipeline.db.DBWired.load_config() docstring
		'central_db': see ampel.pipeline.db.DBWired.plug_central_db() docstring
		'mongodb_uri': URI of the server hosting mongod.
		   Example: 'mongodb://user:password@localhost:27017'
		't3_job_names': optional list of strings. If specified, only job with these names will be run.
		"""
		# Setup logger
		self.logger = LoggingUtils.get_logger(unique=True)
		self.logger.info("Setting up T3Controler")

		# Setup instance variable referencing ampel databases
		self.plug_databases(self.logger, mongodb_uri, config, central_db)

		scheduler = self.get_scheduler()

		for job_name, job_doc in self.config["t3_jobs"].items():

			if t3_job_names is not None and job_name not in t3_job_names:
				continue

			t3_job = T3Job(
				self.config, db_doc=job_doc, logger=self.logger
			)
			
			if job_doc['schedule']['mode'] == "fixed_rate":

				scheduler.every(
					job_doc['schedule']['interval']
				).minutes.do(
					self.launch_t3_job, 
					t3_job
				)

			elif job_doc['schedule']['mode'] == "fixed_time":

				scheduler.every().day.at(
					job_doc['schedule']['time']
				).do(
					self.launch_t3_job, 
					t3_job
				)

			else:
				raise ValueError("Unknown scheduling mode")


	def launch_t3_job(self, job_config_obj):
		"""
		"""
		job_thread = threading.Thread(
			target=T3Executor.run_job, 
			args=(
				self.config, self.photo_col, self.main_col, 
				job_config_obj, self.logger
			)
		)
		job_thread.start()

def run():
	raise NotImplementedError
