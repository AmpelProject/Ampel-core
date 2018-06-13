#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/logging/DBJobReporter.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 14.12.2017
# Last Modified Date: 13.06.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import logging, time


class DBJobReporter:
	""" 
	Inserts/Updates a job info entry into the NoSQL DB
	A job entry contains various info about the current job 
	and an array of log entries produced by this job
	"""


	def __init__(self, mongo_collection):
		""" 
		Parameters:
		mongo_collection: instance of pymongo.collection.Collection
		"""
		self.col = mongo_collection
		self.job_name = "Not set"
		self.flush_job_info = False


	def set_has_error(self):
		""" """
		self.has_error = 1


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


	def set_flush_job_info(self):
		"""
		"""
		self.flush_job_info = True


	def get_job_id(self):
		""" 
		"""
		return getattr(self, "job_id", None)


	def insert_new(self, params, tier):
		""" 
		"""

		job_dict = {
			"tier": tier,
			"params": params,
		}

		if hasattr(self, "job_name"):
			job_dict['jobName'] = self.job_name

		if hasattr(self, "arguments"):
			job_dict['arguments'] = self.arguments

		if hasattr(self, "grid_name"):
			job_dict['gridName'] = self.grid_name

		self.job_id = self.col.insert_one(job_dict).inserted_id

		# Returned objectId can later be used to update the inserted document
		return self.job_id


	def set_job_stats(self, key_name, dict_instance):
		""" 
		"""
		self.job_stats = (key_name, dict_instance)


	def push_logs(self, records):
		""" 
		"""
		update_dict = {
			"$push": {
				"records": { 
					"$each": records
				}
			}
		}

		if self.flush_job_info:

			update_dict["$set"] = {
				"duration": int(
					time.time() - self.job_id.generation_time.timestamp()
				)
			}

			if hasattr(self, "has_error"):
				update_dict["$set"]['hasError'] = True

			if hasattr(self, "job_stats"):
				update_dict["$set"][self.job_stats[0]] = self.job_stats[1]


		self.col.update_one(
			{"_id": self.job_id},
			update_dict,
			upsert=True
		)
