#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/logging/DBJobReporter.py
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 14.12.2017
# Last Modified Date: 24.01.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>
import logging
from bson import ObjectId
from ampel.flags.JobFlags import JobFlags

class DBJobReporter:
	""" 
	Inserts/Updates a job info entry into the NoSQL DB
	A job entry contains various info about the current job 
	and an array of log entries produced by this job
	"""

	def __init__(self, db, job_flags=None):
		""" 
		Parameters:
		db: instance of pymongo.database.Database
		job_flags: instance of ampel.flags.JobFlags
		"""
		self.col = db['jobs']
		self.job_flags = JobFlags(0) if job_flags is None else job_flags
		self.job_name = "Not set"


	def add_flags(self, job_flags):
		""" 
		Add flags (common.flags.JobFlags) to this job
		"""
		self.job_flags |= job_flags


	def set_job_name(self, job_name):
		self.job_name = job_name


	def set_grid_name(self, grid_name):
		self.grid_name = grid_name


	def set_arguments(self, args):
		self.arguments = args


	def getJobId(self):
		return getattr(self, "jobId", None)


	def insert_new(self, al_params):
		""" 
		"""
		self.jobId = ObjectId()

		jdict = {
			"_id": self.jobId,
			"jobName": self.job_name,
			"jobFlags": self.job_flags.value,
			"ALParams": al_params
		}

		if hasattr(self, "arguments"):
			jdict['arguments'] = self.arguments

		if hasattr(self, "grid_name"):
			jdict['gridName'] = self.grid_name

		self.col.insert_one(jdict)


	def set_duration(self, duration):
		""" 
		"""
		self.col.update_one(
			{ 
				"_id": self.jobId 
			},
 			{ 
				"$set": {
					"duration": duration
				}
			}
 		)


	def push_logs(self, records):
		""" 
		"""
		self.col.update_one(
			{ 
				"_id": self.jobId 
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
