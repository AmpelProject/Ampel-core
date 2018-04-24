#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/db/DBWired.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 19.03.2018
# Last Modified Date: 19.04.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import time, pymongo

class DBWired:
	""" 
	"""

	def plug_databases(self, logger, db_host='localhost', config_db=None, base_dbs=None):
		"""
		Parameters:
		'db_host': dns name or ip address (with optionally a port number ) 
		           of the server hosting mongod
		'config_db': see plug_config_db() docstring
		'base_dbs': see plug_base_dbs() docstring
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
		if self.plug_base_dbs(base_dbs, logger, db_host):

			# Re-try using mongomock rather than pymongo
			import mongomock
			if self.plug_base_dbs(
				base_dbs, logger, db_host, 
				MongoClient=mongomock.mongo_client.MongoClient,
				Database=mongomock.database.Database
			):
				raise ValueError("Illegal type provided for argument 'base_dbs'")


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
			-> string: a pymongo MongoClient will be instanciated (using 'db_host') 
			   and a pymongo.database.Database instance created using the name given by config_db
			-> MongoClient instance: a database instance with name 'Ampel_config' will be loaded using 
			   the provided MongoClient instance (can originate from pymongo or mongomock)
			-> Database instance (pymongo or mongomock): provided database will be used

		"""

		# Default setting
		if config_db is None:
			self.mongo_client = MongoClient(db_host)
			self.config_db = self.mongo_client["Ampel_config"]

		# The config database name was provided
		elif type(config_db) is str:
			self.mongo_client = MongoClient(db_host)
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


	def plug_base_dbs(
		self, base_dbs, logger, db_host='localhost',
		MongoClient=pymongo.mongo_client.MongoClient,
		Database=pymongo.database.Database
	):
		"""		
		setup output database (will typically contain the collections 'transients' and 'jobs')
		Parameter 'base_dbs' must be either:

			-> MongoClient instance (pymongo or mongomock): 
			   the provided instance will be used, whereby databases and collections will be
			   loaded using the values defined in Ampel_config -> global -> dbSpecs -> databases

			-> a dict instance:
				-> keys: as of April 2018, following keys can be used:
				   'transients', 'jobs', 'stats', 'troubles' 
				   (see Ampel_config -> global -> dbSpecs -> databases)
				   If the database or the associated collection
				-> values: can be either:
					* a string: a database with the provided name will be loaded or created.
					  This setting basically overules the db name defined in 
					  Ampel_config -> global -> dbSpecs -> databases -> <dict key> -> dbName
					  A collection will be instanciated using the collection name defined in the conf entry
					  Ampel_config -> global -> dbSpecs -> databases -> <dict key> -> collectionName
				   	  Ampel will ensure that the collection has the right indexes
					* or Database instance (pymongo or mongomock).
					  The provided db instance will be used and a collection will be instanciated
                      using the collection name defined in the conf entry 
					  Ampel_config -> global -> dbSpecs -> databases -> <dict key> -> collectionName
					  Please note that using this option, Ampel will not ensure that 
					  the loaded collection uses the right indexes.
					* or dict with the same structure than Ampel_config -> global -> dbSpecs -> databases -> <dict key>.
				-> example:
					{
						'transients': 'test_transients', 
						'jobs': 'test_jobs'
					}
		"""

		# Load transient DB based on entries from config DB
		if base_dbs is None:
			self.plug_default_base_dbs(
				self.mongo_client if hasattr(self, 'mongo_client') 
				else MongoClient(db_host), 
				logger
			)

		# A reference to a MongoClient instance (pymongo or mongomock) was provided
		elif type(base_dbs) is MongoClient:
			self.plug_default_base_dbs(base_dbs, logger)

		elif type(base_dbs) is dict:

			# Feedback
			logger.info("Customized base DB(s) was provided")

			# Get mongoclient if not instanciated previously	
			mongo_client = (
				MongoClient(db_host) if not hasattr(self, 'mongo_client') 
				else self.mongo_client
			)

			dbs_config = self.global_config['dbSpecs']['databases']

			# Robustness: check that custom dict contains only known db labels
			dbs_names = dbs_config.keys()
			for key in base_dbs.keys():
				if not key in dbs_names:
					raise ValueError("Unknown database name '%s'" % key)
					
			# Loop through base db specs (loaded from config db)
			for key in dbs_config.keys():
			
				dict_value = dbs_config[key] if not key in base_dbs else base_dbs[key]

				# DB name was provided
				if type(dict_value) is str:
					if key in base_dbs:
						config = dbs_config[key].copy()
						config['dbName'] = dict_value
					else:
						config = dbs_config[key]
					setattr(
						self, key + "_col",
						self.get_or_create_db(mongo_client, config, logger)
					)

				# Collection instance was provided
				elif type(dict_value) is Database:
					setattr(
						self, key + "_col", 
						dict_value[dbs_config[key]['collectionName']]
					)
	
				# Dict was provided 
				# Requires same format as Ampel_config -> global -> dbSpecs -> databases -> <dict key>
				elif type(dict_value) is dict:
					setattr(
						self, 
						key + "_col", 
						self.get_or_create_db(mongo_client, dict_value, logger)
					)

				else:

					# Illegal type for list member
					logger.warn(
						"base_dbs[%s] dict value is neither string nor %s. Type: %s" % 
						(key, Database, type(dict_value))
					)

					return True

		# Illegal argument type
		else:
			raise ValueError(
				"type(base_dbs) is neither %s nor dict" % MongoClient
			)


	def plug_default_base_dbs(self, mongo_client, logger):
		"""
		Plug central databases and collections using default values 
		from Ampel_config -> global -> dbSpecs -> databases
		"""

		dbs_config = self.global_config['dbSpecs']['databases']

		for key in dbs_config.keys():
			setattr(
				self, 
				key + "_col", 
				self.get_or_create_db(mongo_client, dbs_config[key], logger)
			)


	def get_or_create_db(self, mongo_client, db_config, logger):
		"""
		Plug central databases and collections using conf values provided by db_config
		"""

		existing_db_names = mongo_client.database_names()
		db_name = db_config['dbName']
		db = mongo_client[db_name]
		existing_col_names = db.collection_names()
		col_name = db_config['collectionName']


		# New DB / collection
		if (
			(
				not db_name in existing_db_names or
				not col_name in existing_col_names
			) and
			'indexes' in db_config
		):

			logger.info("New collection detected, creating indexes")
			col = db.create_collection(col_name)

			for d in db_config['indexes']:
				if 'options' in d:
					col.create_index(d['field'], **d['options'])
				else:
					col.create_index(d['field'])
		else:
			col = db[col_name]

		return col


	def get_tran_col(self):
		# pylint: disable=no-member
		return self.transients_col


	def get_stat_col(self):
		# pylint: disable=no-member
		return self.stats_col


	def get_job_col(self):
		# pylint: disable=no-member
		return self.jobs_col


	def get_trouble_col(self):
		# pylint: disable=no-member
		return self.troubles_col
