#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/config/MockConfigDB.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 01.03.2018
# Last Modified Date: 08.03.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import json, mongomock
from ampel.pipeline.logging.AmpelLogger import AmpelLogger

class MockConfigDB:

	config_db_collection = [
		'channels', 
		'global', 
		't0_filters', 
		't2_run_config', 
		't2_units', 
		't3_jobs', 
		't3_run_config', 
		't3_units'
	]

	def __init__(self, folder, db_name="Ampel_config", logger=None):

		self.logger = AmpelLogger.get_logger() if logger is None else logger
		mc = mongomock.MongoClient()
		self.db = mc[db_name]
	
		# Loop through config collections
		for col in MockConfigDB.config_db_collection:

			# Generate file path
			filepath = "%s/%s/config.json" % (folder,col)
			self.logger.info("Importing db docs from %s" % filepath)

			# Open exported json file
			with open(filepath, "r") as data_file:

				jsarray = json.load(data_file)

				# Remove $oid if present (new _id will be generated on insert)
				for el in jsarray:
					if type(el['_id']) is dict:
						del el['_id']

				# Inserting an empty json doc raises an exception
				if len(jsarray) == 0:
					continue

				# Insert all docs
				self.db[col].insert_many(jsarray)


	def get_config_db(self):
		return self.db
