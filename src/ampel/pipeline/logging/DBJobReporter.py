#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/logging/DBJobReporter.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 14.12.2017
# Last Modified Date: 17.03.2018
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
		self.job_name = job_name


	def set_grid_name(self, grid_name):
		self.grid_name = grid_name


	def set_arguments(self, args):
		self.arguments = args


	def get_job_id(self):
		return getattr(self, "job_id", None)


	def insert_new(self, al_params, transient_col=None):
		""" 
		Optional:
		transient_col: mongo collection containing the transient documents 
		(used for keeping track of db stats)
		"""
		self.job_id = ObjectId()

		jdict = {
			"_id": self.job_id,
			"jobName": self.job_name,
			"jobFlags": self.job_flags.value,
			"params": al_params,
		}

		if transient_col is not None:

			colstats = transient_col.database.command(
				"collstats", transient_col.name
			)

			jdict["dbStats"] = {
				'count': colstats['count'],
				'size': colstats['size'],
				'storageSize': colstats['storageSize'],
				'totalIndexSize': colstats['totalIndexSize']
			}


		if hasattr(self, "arguments"):
			jdict['arguments'] = self.arguments

		if hasattr(self, "grid_name"):
			jdict['gridName'] = self.grid_name

		return self.col.insert_one(jdict)


	def set_job_stats(self, arg_dict):
		""" 
		"""
		self.col.update_one(
			{ 
				"_id": self.job_id 
			},
 			{ 
				"$set": {
					"jobStats": arg_dict
				}
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
