#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/db/DBWired.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 19.03.2018
# Last Modified Date: 31.05.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import time, pymongo
from pymongo.errors import CollectionInvalid
from ampel.flags.AlDocTypes import AlDocTypes
from ampel.pipeline.db.DBIndexCreator import DBIndexCreator

class DBWired:
	""" 
	"""

	def plug_databases(self, logger, mongodb_uri='localhost', config=None, central_db=None):
		"""
		Parameters:
		'mongodb_uri': URI of server hosting mongod. 
		   Example: 'mongodb://user:password@localhost:27017'
		'config': see plug_config_db() docstring
		'central_db': see plug_central_db() docstring
		"""

		# Setup instance variable referencing the input database
		if self.plug_config_db(logger, mongodb_uri, config):

			# Re-try using mongomock rather than pymongo
			import mongomock
			if self.plug_config_db(
				logger, mongodb_uri, config, 
				MongoClient=mongomock.mongo_client.MongoClient,
				Database=mongomock.database.Database
			):
				raise ValueError("Illegal type provided for argument 'config'")

		# Load general config using the input config db
		self.global_config = {}
		for doc in self.config_db['global'].find({}):
			self.global_config[doc['_id']] = doc

		# Setup instance variables referencing the output databases
		if self.plug_central_db(central_db, logger, mongodb_uri):

			# Re-try using mongomock rather than pymongo
			import mongomock
			if self.plug_central_db(
				central_db, logger, mongodb_uri, 
				MongoClient=mongomock.mongo_client.MongoClient,
				Database=mongomock.database.Database
			):
				raise ValueError("Illegal type provided for argument 'central_db'")


	def plug_config_db(
		self, logger, mongodb_uri='localhost', config=None,
		MongoClient=pymongo.mongo_client.MongoClient,
		Database=pymongo.database.Database
	):
		"""
		Sets up the database containing the Ampel config collections.
		'config': 
		    Either:
			-> None: default settings will be used 
			   (pymongo MongoClient instance using 'mongodb_uri' and config db name 'Ampel_config')
			-> string:
			    * if string ends with '.json': json file will be loaded into a mongomock db 
				  which will be used as config db.
				  self.config_db will reference an instance of mongomock.database.Database. 
			    * otherwise: the string value will be used to retrieve the config db by name. 
				  self.config_db will reference an instance of pymongo.database.Database. 
				  (Please note that 'mongodb_uri' will be use for the mongoclient instantiation)
			-> MongoClient instance: a database instance with name 'Ampel_config' will be loaded using 
			   the provided MongoClient instance (can originate from pymongo or mongomock)
			-> Database instance (pymongo or mongomock): provided database will be used

		"""

		# Default setting
		if config is None:
			self.mongo_client = MongoClient(mongodb_uri, maxIdleTimeMS=1000)
			self.config_db = self.mongo_client["Ampel_config"]

		# The config database name was provided
		elif type(config) is str:
			if config.endswith(".json"):
				from ampel.pipeline.db.MockDBUtils import MockDBUtils
				self.config_db = MockDBUtils.load_db_from_file(config, logger)
				logger.info("Mock config db created using %s" % config)
			else:
				self.mongo_client = MongoClient(mongodb_uri, maxIdleTimeMS=1000)
				self.config_db = self.mongo_client[config]

		# A reference to a MongoClient instance was provided
		# -> Provided config type can be (pymongo or mongomock).mongo_client.MongoClient
		elif type(config) is MongoClient:
			self.config_db = config["Ampel_config"]

		# A reference to a database instance (pymongo or mongomock) was provided
		# -> Provided config_db type can be (pymongo or mongomock).database.Database
		elif type(config) is Database:
			self.config_db = config

		# Illegal argument
		else:
			logger.warn(
				"Provided argument value for 'config' is neither " + 
				"string nor %s nor %s" % (MongoClient, Database)
			)
			return True

		return False


	def plug_central_db(
		self, arg, logger, mongodb_uri='localhost',
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
					else MongoClient(mongodb_uri, maxIdleTimeMS=1000)
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
				MongoClient(mongodb_uri, maxIdleTimeMS=1000) if not hasattr(self, 'mongo_client') 
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
			try:
				self.photo_col = db.create_collection("photo")
				logger.info("Creating new photo collection")
				DBIndexCreator.create_photo_indexes(self.photo_col)
			except CollectionInvalid:
				# Catch 'CollectionInvalid: collection * already exists' 
				# that can occur when multiple jobs are created simultaneoulsy
				pass

		if "main" in existing_col_names:
			self.main_col = db["main"]
		else:
			try:
				self.main_col = db.create_collection("main")
				logger.info("Creating new main collection")
				DBIndexCreator.create_main_indexes(self.main_col)
			except CollectionInvalid:
				# Catch 'CollectionInvalid: collection * already exists' 
				# that can occur when multiple jobs are created simultaneoulsy
				pass


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
