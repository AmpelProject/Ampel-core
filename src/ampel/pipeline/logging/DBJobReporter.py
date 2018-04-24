#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/logging/DBJobReporter.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 14.12.2017
# Last Modified Date: 23.04.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import logging
from ampel.flags.JobFlags import JobFlags


class DBJobReporter:
	""" 
	Inserts/Updates a job info entry into the NoSQL DB
	A job entry contains various info about the current job 
	and an array of log entries produced by this job
	"""


	def __init__(self, mongo_collection, job_flags=None):
		""" 
		Parameters:
		mongo_collection: instance of pymongo.collection.Collection
		job_flags: instance of ampel.flags.JobFlags
		"""
		self.col = mongo_collection
		self.job_flags = JobFlags(0) if job_flags is None else job_flags
		self.job_name = "Not set"


	def add_flags(self, job_flags):
		""" 
		Add flags (ampel.flags.JobFlags) to this job
		"""
		self.job_flags |= job_flags


	def set_job_name(self, job_name):
		""" 
		"""
		self.job_name = job_name


	def set_grid_name(self, grid_name):
		""" 
		"""
		self.grid_name = grid_name


	def set_arguments(self, args):
		""" 
		"""
		self.arguments = args


	def get_job_id(self):
		""" 
		"""
		return getattr(self, "job_id", None)


	def insert_new(self, params, tier):
		""" 
		"""

		job_dict = {
			"jobName": self.job_name,
			"jobFlags": self.job_flags.value,
			"tier": tier,
			"params": params,
		}

		if hasattr(self, "arguments"):
			job_dict['arguments'] = self.arguments

		if hasattr(self, "grid_name"):
			job_dict['gridName'] = self.grid_name

		self.job_id = self.col.insert_one(job_dict).inserted_id

		# Returned objectId can later be used to update the inserted document
		return self.job_id


	def update_job_info(self, arg_dict):
		""" 
		"""
		self.col.update_one(
			{ 
				"_id": self.job_id 
			},
 			{ 
				"$set": arg_dict
			}
 		)


	def push_logs(self, records):
		""" 
		"""
		self.col.update_one(
			{ 
				"_id": self.job_id 
			},
			{
				"$set": {
					"jobFlags": self.job_flags.value
				},
				"$push": {
					"records": { 
						"$each": records
					}
				}
			},
			upsert=True
		)
