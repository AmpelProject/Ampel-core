#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/t3/T3Controller.py
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 26.02.2018
# Last Modified Date: 26.02.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from ampel.pipeline.logging.LoggingUtils import LoggingUtils
import schedule, time, multiprocessing

class T3Controller:
	"""
	"""

	def __init__(self, db, logger=None, collection="main"):
		"""
		"""
		self.col = db[collection]
		self.logger = LoggingUtils.get_logger() if logger is None else logger

		t3_config = self.col.get_collection("config").find({}).next()

		for t3_config_el in t3_config['run']:
			
			if t3_config_el['schedule']['mode'] == "fixed_rate":

				schedule.every(
					t3_config_el['schedule']['interval']
				).minutes.do(
					self.launch_t3_job, 
					t3_config_el
				)

			elif t3_config_el['schedule']['mode'] == "fixed_time":

				schedule.every().day.at(
					t3_config_el['schedule']['time']
				).do(
					self.launch_t3_job, 
					t3_config_el
				)

			else:
				raise ValueError("Unknown schedule mode")


	def launch_t3_job(self, t3_config_el):
		pass
		

	def start(self):
		"""
		"""
		while True:
			schedule.run_pending()
			time.sleep(1)
