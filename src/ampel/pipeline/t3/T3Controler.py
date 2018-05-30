#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/t3/T3Controler.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 26.02.2018
# Last Modified Date: 28.05.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import schedule, time, threading
from ampel.pipeline.db.DBWired import DBWired
from ampel.pipeline.t3.T3Executor import T3Executor
from ampel.pipeline.t3.conf.T3JobConfig import T3JobConfig
from ampel.pipeline.common.Schedulable import Schedulable
from ampel.pipeline.logging.LoggingUtils import LoggingUtils

class T3Controler(DBWired, Schedulable):
	"""
	"""

	def __init__(
		self, config_db=None, central_db=None, mongo_uri=None, t3_jobs=None
	):
		"""
		"""
		# Setup logger
		self.logger = LoggingUtils.get_logger(unique=True)
		self.logger.info("Setting up T3Controler")

		# Setup instance variable referencing ampel databases
		self.plug_databases(self.logger, mongo_uri, config_db, central_db)

		jobs_cursor = self.config_db["t3_jobs"].find(
			{} if t3_jobs is None else {'_id': {'$in': t3_jobs}}
		)

		scheduler = self.get_scheduler()

		for t3_doc in jobs_cursor:

			job_config = T3JobConfig(
				self.config_db, db_doc=t3_doc, logger=self.logger
			)
			
			if t3_doc['schedule']['mode'] == "fixed_rate":

				scheduler.every(
					t3_doc['schedule']['interval']
				).minutes.do(
					self.launch_t3_job, 
					job_config
				)

			elif t3_doc['schedule']['mode'] == "fixed_time":

				scheduler.every().day.at(
					t3_doc['schedule']['time']
				).do(
					self.launch_t3_job, 
					job_config
				)

			else:
				raise ValueError("Unknown scheduling mode")


	def launch_t3_job(self, job_config_obj):
		"""
		"""
		job_thread = threading.Thread(
			target=T3Executor.run_job, 
			args=(
				self.config_db, self.photo_col, self.main_col, 
				job_config_obj, self.logger
			)
		)
		job_thread.start()

def run():
	raise NotImplementedError
