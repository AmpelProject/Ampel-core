#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/db/DBWired.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 19.03.2018
# Last Modified Date: 23.05.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import time, pymongo
from ampel.flags.AlDocTypes import AlDocTypes
from ampel.pipeline.db.DBIndexCreator import DBIndexCreator

class DBWired:
	""" 
	"""

	def plug_databases(self, logger, db_host='localhost', config_db=None, central_db=None):
		"""
		Parameters:
		'db_host': dns name or ip address (with optionally a port number) of server hosting mongod
		'config_db': see plug_config_db() docstring
		'central_db': see plug_central_db() docstring
		"""

		# Setup instance variable referencing the input database
		if self.plug_config_db(logger, db_host, config_db):

			# Re-try using mongomock rather than pymongo
			import mongomock
			if self.plug_config_db(
				logger, db_host, config_db, 
				MongoClient=mongomock.mongo_client.MongoClient,
				Database=mongomock.database.Database
			):
				raise ValueError("Illegal type provided for argument 'config_db'")

		# Load general config using the input config db
		self.global_config = {}
		for doc in self.config_db['global'].find({}):
			self.global_config[doc['_id']] = doc

		# Setup instance variables referencing the output databases
		if self.plug_central_db(central_db, logger, db_host):

			# Re-try using mongomock rather than pymongo
			import mongomock
			if self.plug_central_db(
				central_db, logger, db_host, 
				MongoClient=mongomock.mongo_client.MongoClient,
				Database=mongomock.database.Database
			):
				raise ValueError("Illegal type provided for argument 'central_db'")


	def plug_config_db(
		self, logger, db_host='localhost', config_db=None,
		MongoClient=pymongo.mongo_client.MongoClient,
		Database=pymongo.database.Database
	):
		"""
		Sets up the database containing the Ampel config collections.
		'config_db': 
		    Either:
			-> None: default settings will be used 
			   (pymongo MongoClient instance using 'db_host' and config db name 'Ampel_config')
			-> string: a pymongo MongoClient will be instantiated (using 'db_host') 
			   and a pymongo.database.Database instance created using the name given by config_db
			-> MongoClient instance: a database instance with name 'Ampel_config' will be loaded using 
			   the provided MongoClient instance (can originate from pymongo or mongomock)
			-> Database instance (pymongo or mongomock): provided database will be used

		"""

		# Default setting
		if config_db is None:
			self.mongo_client = MongoClient(db_host, maxIdleTimeMS=1000)
			self.config_db = self.mongo_client["Ampel_config"]

		# The config database name was provided
		elif type(config_db) is str:
			self.mongo_client = MongoClient(db_host, maxIdleTimeMS=1000)
			self.config_db = self.mongo_client[config_db]

		# A reference to a MongoClient instance was provided
		# -> Provided config_db type can be (pymongo or mongomock).mongo_client.MongoClient
		elif type(config_db) is MongoClient:
			self.config_db = config_db["Ampel_config"]

		# A reference to a database instance (pymongo or mongomock) was provided
		# -> Provided config_db type can be (pymongo or mongomock).database.Database
		elif type(config_db) is Database:
			self.config_db = config_db

		# Illegal argument
		else:
			logger.warn(
				"Provided argument value for 'config_db' is neither " + 
				"string nor %s nor %s" % (MongoClient, Database)
			)
			return True

		return False


	def plug_central_db(
		self, arg, logger, db_host='localhost',
		MongoClient=pymongo.mongo_client.MongoClient,
		Database=pymongo.database.Database
	):
		"""		
		setup output database (will typically contain the collections 'transients' and 'jobs')
		Parameter 'arg' must be either:

			-> MongoClient instance (pymongo or mongomock): the provided instance will be used
			   If the required collections do not exist, Ampel will create them and 
			   ensure that they have the right indexes

			-> A string: a database with the provided name will be loaded or created.
			   If the required collections do not exist, Ampel will create them and 
			   ensure that they have the right indexes
			 
			-> A Database instance (pymongo or mongomock).
			   If the required collections do not exist, Ampel will create them and 
			   ensure that they have the right indexes
		"""

		# Load transient DB based on entries from config DB
		if arg is None:
			self.set_vars(
				logger, mc = (
					self.mongo_client if hasattr(self, 'mongo_client') 
					else MongoClient(db_host, maxIdleTimeMS=1000)
				)
			)

		# A reference to a MongoClient instance (pymongo or mongomock) was provided
		elif type(arg) is MongoClient:
			logger.info("Customized MongoClient was provided")
			self.set_vars(logger, mc=arg)

		elif type(arg) is str:

			logger.info("Customized central DB name was provided: %s" % arg)

			# Get mongoclient if not instantiated previously	
			mongo_client = (
				MongoClient(db_host, maxIdleTimeMS=1000) if not hasattr(self, 'mongo_client') 
				else self.mongo_client
			)

			self.set_vars(logger, db=mongo_client[arg])

		elif type(arg) is Database:
		
			logger.info("Customized central Database instance")
			self.set_vars(logger, db=arg)

		# Illegal argument type
		else:
			raise ValueError("Invalid argument")


	def set_vars(self, logger, mc=None, db=None):
		"""
		Plug central database and collections using default values 
		"""

		if mc is None and db is None:
			raise ValueError("Invalid arguments")

		if mc is not None:
			db = mc["Ampel"]

		existing_col_names = db.collection_names()

		if "photo" in existing_col_names:
			self.photo_col = db["photo"]
		else:
			logger.info("Creating new photo collection")
			self.photo_col = db.create_collection("photo")
			DBIndexCreator.create_photo_indexes(self.photo_col)

		if "main" in existing_col_names:
			self.main_col = db["main"]
		else:
			logger.info("Creating new main collection")
			self.main_col = db.create_collection("main")
			DBIndexCreator.create_main_indexes(self.main_col)


		if "jobs" in existing_col_names:
			self.jobs_col = db["jobs"]
		else:
			self.jobs_col = db.create_collection(
				'jobs', storageEngine={
					'wiredTiger':{
						'configString':'block_compressor=zlib'
					}
				}
			)

		self.troubles_col = db.client["Ampel_troubles"]['docs']


	def get_main_col(self):
		# pylint: disable=no-member
		return self.main_col


	def get_photo_col(self):
		# pylint: disable=no-member
		return self.photo_col


	def get_job_col(self):
		# pylint: disable=no-member
		return self.jobs_col


	def get_trouble_col(self):
		# pylint: disable=no-member
		return self.troubles_col
