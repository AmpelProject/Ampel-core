#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : /Users/hu/Documents/ZTF/Ampel/src/ampel/pipeline/common/db/DBJobReporter.py
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 14.12.2017
# Last Modified Date: 14.12.2017
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>
import logging
from bson import ObjectId
from ampel.pipeline.common.flags.JobFlags import JobFlags

class DBJobReporter:
	""" 
		Inserts/Updates a job info entry into the NoSQL DB
		A job entry contains various info about the current job 
		and the associated array of log entries 
	"""

	def __init__(self, mongo_client, pipeline_version):
		""" 
		"""
		db = mongo_client['events']
		self.col = db['jobs']
		self.logger = logging.getLogger("Ampel")
		self.flags = JobFlags(0)
		self.job_name = "Not set"
		self.ppl_version = pipeline_version

	def add_flags(self, job_flags):
		""" 
			Add flags (common.flags.JobFlags) to this job
		"""
		self.flags |= job_flags

	def set_job_name(self, job_name):
		self.job_name = job_name

	def set_grid_name(self, grid_name):
		self.grid_name = grid_name

	def set_arguments(self, args):
		self.arguments = args

	def getJobId(self):
		if hasattr(self, "jobId"):
			return self.jobId
		return None

	def insert_new(self, ppl_processor):
		""" 
		"""
		self.jobId = ObjectId()

		jdict = {
			"_id": self.jobId,
			"jobName": self.job_name,
			"PPLversion": self.ppl_version,
			"flags": self.flags.value,
			"PPLparams": {
				"dispatcherId": str(ppl_processor.dispatcher.__class__)
			}
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
			{ "_id": self.jobId },
 			{ "$set": {"duration": duration}}
 		)

	def push_logs(self, records):
		""" 
		"""
		self.col.update_one(
			{ "_id": self.jobId },
			{
				"$set": {"flags": self.flags.value},
				"$pushAll": {"records": records}
			},
			upsert=True
		)
