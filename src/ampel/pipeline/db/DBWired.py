#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/db/DBWired.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 19.03.2018
# Last Modified Date: 19.03.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import time, pymongo

class DBWired:
	""" 
	"""

	def plug_databases(self, db_host='localhost', input_db=None, output_db=None):
		"""
		Parameters:
		'db_host': dns name or ip address (plus optinal port) of the server hosting mongod
		'input_db': the database containing the Ampel config collections.
		    Either:
			-> None: default settings will be used 
			   (pymongo MongoClient instance using 'db_host' and db name 'Ampel_config')
			-> string: pymongo MongoClient instance using 'db_host' 
			   and database with name identical to 'input_db' value
			-> MongoClient instance: database with name 'Ampel_config' will be loaded from 
			   the provided MongoClient instance (can originate from pymongo or mongomock)
			-> Database instance (pymongo or mongomock): provided database will be used
		'output_db': the output database (will typically contain the collections 'transients' and 'logs')
		    Either:
			-> MongoClient instance (pymongo or mongomock): the provided instance will be used 
			-> dict: (example: {'transients': 'test_transients', 'logs': 'test_logs'})
				-> must have the keys 'transients' and 'logs'
				-> values must be either string or Database instance (pymongo or mongomock)
		"""

		# Setup instance variable referencing the input database
		self.plug_input_db(input_db, db_host)

		# Check if previous command did set up the config_db correctly
		if not hasattr(self, 'config_db'):

			# Re-try using mongomock rather than pymongo
			import mongomock
			if self.plug_input_db(
				input_db, db_host, 
				MongoClient=mongomock.mongo_client.MongoClient,
				Database=mongomock.database.Database
			):
				raise ValueError("Illegal type provided for argument 'input_db'")

		# Load general config using the input config db
		self.global_config = {}
		for doc in self.config_db['global'].find({}):
			self.global_config[doc['_id']] = doc

		# Setup instance variables referencing the output databases
		self.plug_output_dbs(output_db, db_host)

		# Check if previous command did set up the output databases correctly
		if not hasattr(self, 'tran_db'):

			# Re-try using mongomock rather than pymongo
			import mongomock
			if self.plug_output_dbs(
				output_db, db_host, 
				MongoClient=mongomock.mongo_client.MongoClient,
				Database=mongomock.database.Database
			):
				raise ValueError("Illegal type provided for argument 'output_db'")


	def plug_input_db(
		self, input_db=None, db_host='localhost', 
		MongoClient=pymongo.mongo_client.MongoClient,
		Database=pymongo.database.Database
	):
		"""
		Sets up the database containing the Ampel config collections.
		Parameter 'input_db' must be one of these types: 
			-> None: default settings will be used 
			   (pymongo MongoClient instance using 'db_host' and db name 'Ampel_config')
			-> string: pymongo MongoClient instance using 'db_host' 
			   and database with name identical to 'input_db' value
			-> MongoClient instance: database with name 'Ampel_config' will be loaded from 
			   the provided MongoClient instance (can originate from pymongo or mongomock)
			-> Database instance (pymongo or mongomock): provided database will be used
		"""

		# Default setting
		if input_db is None:
			self.mongo_client = MongoClient(db_host)
			self.config_db = self.mongo_client["Ampel_config"]

		# The config database name was provided
		elif type(input_db) is str:
			self.mongo_client = MongoClient(db_host)
			self.config_db = self.mongo_client[input_db]

		# A reference to a MongoClient instance was provided
		# -> Provided input_db type can be (pymongo or mongomock).mongo_client.MongoClient
		elif type(input_db) is MongoClient:
			self.config_db = input_db["Ampel_config"]

		# A reference to a database instance (pymongo or mongomock) was provided
		# -> Provided input_db type can be (pymongo or mongomock).database.Database
		elif type(input_db) is Database:
			self.config_db = input_db

		# Illegal argument
		else:
			raise ValueError("input_db type is wether str, %s or %s" % (MongoClient, Database))


	def plug_output_dbs(
		self, output_db, db_host='localhost',
		MongoClient=pymongo.mongo_client.MongoClient,
		Database=pymongo.database.Database
	):
		"""		
		setup output database (will typically contain the collections 'transients' and 'logs')
		Parameter 'output_db' must have of these types:
			-> MongoClient instance (pymongo or mongomock): the provided instance will be used 
			-> dict: (example: {'transients': 'test_transients', 'logs': 'test_logs'})
				-> must have the keys 'transients' and 'logs'
				-> values must be either string or Database instance (pymongo or mongomock)
		"""

		# Load transient DB based on entries from config DB
		if output_db is None:
			self.setattr_output_dbs(
				self.mongo_client if hasattr(self, 'mongo_client') else MongoClient(db_host)
			)

		# A reference to a MongoClient instance (pymongo or mongomock) was provided
		elif type(output_db) is MongoClient:
			self.setattr_output_dbs(output_db)

		elif type(output_db) is dict:

			# Robustness check
			if len(output_db) != 2 or "transients" not in output_db or "logs" not in output_db:
				raise ValueError(
					'output_db dict must have 2 keys: "transients" and "logs"'
				)

			if not hasattr(self, 'mongo_client'):
				self.mongo_client = MongoClient(db_host)

			self.setattr_output_db(output_db["transients"], "tran_db", self.mongo_client)
			self.setattr_output_db(output_db["logs"], "log_db", self.mongo_client)

			db_specs = self.global_config['dbSpecs']

			# pylint: disable=no-member
			self.tran_col = self.tran_db[db_specs['transients']['collectionName']]

			# pylint: disable=no-member
			self.log_col = self.log_db[db_specs['logs']['collectionName']]
				
		# Illegal argument type
		else:
			raise ValueError(
				"type(output_db) is wether str, %s or %s" % (MongoClient, Database)
			)


	def setattr_output_db(
		self, output_db, local_varname, mongo_client,
		Database=pymongo.database.Database
	):
		"""
		"""

		# Collection name was provided
		if type(output_db) is str:
			setattr(self, local_varname, mongo_client[output_db])

		# Collection instance was provided
		elif type(output_db) is Database:
			setattr(self, local_varname, output_db)

		else:
			# Illegal type for list member
			raise ValueError(
				"output_db dict values are wether str, %s or %s" % Database
			)
				

	def setattr_output_dbs(self, mongo_client):
		"""
		"""

		db_specs = self.global_config['dbSpecs']

		self.tran_db = mongo_client[
			db_specs['transients']['dbName']
		]

		self.tran_col = self.tran_db[
			db_specs['transients']['collectionName']
		]

		log_db = mongo_client[
			db_specs['logs']['dbName']
		]

		self.log_col = log_db[
			db_specs['logs']['collectionName']
		]
