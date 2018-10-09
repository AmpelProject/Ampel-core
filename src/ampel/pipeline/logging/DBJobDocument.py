#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel/src/ampel/pipeline/logging/DBJobDocument.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 26.09.2018
# Last Modified Date: 26.09.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from datetime import datetime
from ampel.pipeline.db.AmpelDB import AmpelDB

class DBJobDocument():
	"""
	"""

	def __init__(self, tier, col="events"):
		""" 
		:param int tier: value (0,1,2,3) indicating at which ampel tier level logging is done
		:param str col: name of db collection to use (default 'jobs'). 
		Collection is retrieved using AmpelDB.get_collection()
		"""
		self.tier = tier
		self.col = AmpelDB.get_collection(col)
		self.run_ids = []


	def set_job_info(self, job_info):
		""" """
		self.job_info = job_info


	def add_run_id(self, run_id):
		"""
		:param int run_id: 
		"""
		self.run_ids.append(run_id)


	def publish(self):
		"""
		This action flushes the logs as well
		"""

		AmpelDB.get_collection('jobs').update_one(
			{
				'_id': int(
					datetime.today().strftime('%Y%m%d')
				)
			},
			{
				'$push': {
					'jobs': {
						'tier': self.tier,
						'dt': datetime.utcnow().timestamp(),
						'runId': self.run_ids[0] if len(self.run_ids) == 1 else self.run_ids,
						**self.job_info
					}
				}
			},
			upsert=True
		)
		self.job_info = {}
