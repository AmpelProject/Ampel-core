#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/db/MockDBUtils.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 14.12.2017
# Last Modified Date: 02.06.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from glob import glob
import json, os, mongomock

class MockDBUtils:

	config_col_names = (
		'channels', 'global', 't0_filters', 
		't2_run_config', 't2_units', 
		't3_jobs', 't3_run_config', 't3_units'
	)


	@staticmethod
	def load_db_from_folder(path):
		"""
		"""
		mc = mongomock.MongoClient()
		db = mc["Ampel_mockconfig"]

		for folder in glob(path + "/*"):
			for jsonfile in glob(folder+"/*.json"):
				with open(jsonfile, "r") as f:
					db[os.path.basename(folder)].insert_many(
						json.load(f)
					)

		return db

	
	@staticmethod
	def dump_db_to_file(db, path_to_json_file):
		"""
		"""
		config = {}

		for colname in MockDBUtils.config_col_names:

			config[colname] = {}

			for el in db[colname].find({}):
				config[colname][el.pop('_id')] = el
		
		with open(path_to_json_file, "w") as f:
			json.dump(config, f, indent=4)



	@staticmethod
	def load_db_from_file(jsonfile, logger):
		"""
		"""
		mc = mongomock.MongoClient()
		db = mc["Ampel_mockconfig"]

		with open(jsonfile, "r") as f:
			in_dict = json.load(f)
			for key in MockDBUtils.config_col_names:
				try:
					db[key].insert_many(in_dict[key])
				except KeyError:
					logger.warn("Collection %s not available in config file" % key)	
		
		return db


#	@staticmethod
#	def save_DB(mongo_client):
#		"""
#		"""
#		for dbname in mongo_client.database_names():
#
#		    if dbname.startswith("__"):
#		        continue
#
#		    db = mongo_client.get_database(MockDBUtils.base_path + "/" + dbname)
#		    if not os.path.exists(MockDBUtils.base_path + "/" +dbname):
#		        os.makedirs(MockDBUtils.base_path + "/" + dbname)
#
#		    for colentry in db.collection_names():
#
#		        if colentry.startswith("_") or colentry.rfind(".") != -1:
#		            continue
#
#		        colname = colentry.replace(".__name__", "")
#		        mycol = db.get_collection(colname)
#		        f = open(MockDBUtils.base_path + "/" + dbname+"/"+colname+".json", "w")
#		        f.write(dumps(mycol.find({})))
#		        f.close()

