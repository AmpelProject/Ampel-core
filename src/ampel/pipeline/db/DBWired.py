#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/db/DBWired.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 19.03.2018
# Last Modified Date: 11.06.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import time, pymongo
from pymongo.errors import CollectionInvalid
from ampel.flags.AlDocTypes import AlDocTypes
from ampel.pipeline.db.DBIndexCreator import DBIndexCreator
from types import MappingProxyType

class DBWired:
	""" 
	"""

	config_col_names = {
		'all':	(
			'global', 'channels', 't0_filters',
			't2_units', 't2_run_config', 
			't3_jobs', 't3_run_config', 't3_units'
		),
		0: (
			'global', 'channels', 't0_filters', 
			't2_units', 't2_run_config'
		),
		2: (
			'global', 't2_units', 't2_run_config'
		)
	}


	@staticmethod
	def get_config_from_db(db, tier='all'):
		"""
		"""
		config = {}

		for colname in DBWired.config_col_names[tier]:

			config[colname] = {}

			for el in db[colname].find({}):
				config[colname][el.pop('_id')] = el

		return config


	def plug_databases(self, logger, mongodb_uri='localhost', arg_config=None, central_db=None):
		"""
		Parameters:
		'mongodb_uri': URI of server hosting mongod. 
		   Example: 'mongodb://user:password@localhost:27017'
		'arg_config': see load_config() docstring
		'central_db': see plug_central_db() docstring
		"""

		# Setup instance variable referencing the input database
		if self.load_config(logger, mongodb_uri, arg_config):

			# Re-try using mongomock rather than pymongo
			import mongomock
			if self.load_config(
				logger, mongodb_uri, arg_config, 
				MongoClient=mongomock.mongo_client.MongoClient,
				Database=mongomock.database.Database
			):
				raise ValueError("Illegal type provided for argument 'arg_config'")

		# Load general config using the input config db
		self.global_config = self.config['global']

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


	def load_config(
		self, logger, mongodb_uri='localhost', arg_config=None,
		MongoClient=pymongo.mongo_client.MongoClient,
		Database=pymongo.database.Database
	):
		"""
		Sets up the database containing the Ampel config collections.
		'arg_config': 
		    Either:
			-> None: default settings will be used: config will be loaded from db using 
			   a pymongo MongoClient instance setup with 'mongodb_uri' and config entries 
			   loaded from db with name 'Ampel_config'
			-> string:
			    * if string ends with '.json': json file will be loaded and used as config.
			    * otherwise: config values will be loaded from a db whose name matches with 
				  the provided string ('mongodb_uri' will be use for the mongoclient instantiation)
			-> MongoClient instance: config will be loaded from db with name 'Ampel_config' 
			   using the provided mongo client
			-> Database instance: config will be loaded using the provided database
		"""

		# Default setting
		if arg_config is None:
			self.mongo_client = MongoClient(mongodb_uri, maxIdleTimeMS=1000)
			self.config = DBWired.get_config_from_db(self.mongo_client["Ampel_config"])

		# The config database name was provided
		elif type(arg_config) is str:
			if arg_config.endswith(".json"):
				import json
				with open(arg_config, "r") as f:
					self.config = json.load(f)
				logger.info("Config db loaded using %s" % arg_config)
			else:
				self.mongo_client = MongoClient(mongodb_uri, maxIdleTimeMS=1000)
				self.config = DBWired.get_config_from_db(self.mongo_client[arg_config])
		
		elif isinstance(arg_config, dict) or isinstance(arg_config, MappingProxyType):
			self.config = arg_config

		# A reference to a MongoClient instance was provided
		elif type(arg_config) is MongoClient:
			self.config = DBWired.get_config_from_db(arg_config["Ampel_config"])

		# A reference to a database instance (pymongo or mongomock) was provided
		# -> Provided config_db type can be (pymongo or mongomock).database.Database
		elif type(arg_config) is Database:
			self.config = DBWired.get_config_from_db(arg_config)

		# Illegal argument
		else:
			logger.warn(
				"Provided argument value for 'arg_config' is neither " + 
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
		setup output database (will typically contain the collections 'transients' and 'logs')
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
		self.central_db = db

		self.troubles_col = db.client["Ampel_troubles"]['docs']

		for col_name in ('photo', 'main', 'logs', 'stats', 'runs'):
			if col_name not in existing_col_names:
				logger.info("Creating new %s collection" % col_name)
				DBIndexCreator.create_indexes(db[col_name])
			

	def get_central_col(self, name):
		""" """
		return self.central_db[name]


	def get_trouble_col(self):
		""" """
		return self.troubles_col


	def get_central_db(self):
		""" """
		return self.central_db
