#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : /Users/hu/Documents/ZTF/Ampel/src/ampel/pipeline/common/db/MockDBUtils.py
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 14.12.2017
# Last Modified Date: 14.12.2017
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>
from bson.json_util import dumps
import json, os

class MockDBUtils:

	base_path = "/Users/hu/Documents/ZTF/ampel/test"

	@staticmethod
	def save_DB(mongo_client):
		"""
		"""
		for dbname in mongo_client.database_names():

		    if dbname.startswith("__"):
		        continue

		    db = mongo_client.get_database(MockDBUtils.base_path + "/" + dbname)
		    if not os.path.exists(MockDBUtils.base_path + "/" +dbname):
		        os.makedirs(MockDBUtils.base_path + "/" + dbname)

		    for colentry in db.collection_names():

		        if colentry.startswith("_") or colentry.rfind(".") != -1:
		            continue

		        colname = colentry.replace(".__name__", "")
		        mycol = db.get_collection(colname)
		        f = open(MockDBUtils.base_path + "/" + dbname+"/"+colname+".json", "w")
		        f.write(dumps(mycol.find({})))
		        f.close()


	@staticmethod
	def load_DB(mongo_client):
		"""
		"""
		from glob import glob

		for dbentry in glob(MockDBUtils.base_path + "/*"):
		    dbname = os.path.basename(dbentry)
		    db = mongo_client.get_database(dbname)
		    for jsonfile in glob(dbentry+"/*.json"):
		        f = open(jsonfile, "r")
		        mycol = db.get_collection(os.path.basename(jsonfile).replace(".json", ""))
		        mycol.insert_many(json.loads(f.readline()))
		        f.close()
