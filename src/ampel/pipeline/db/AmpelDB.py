#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/db/AmpelDB.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 16.06.2018
# Last Modified Date: 25.09.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from ampel.pipeline.config.AmpelConfig import AmpelConfig
from ampel.core.flags.AlDocTypes import AlDocTypes

class AmpelDB:
	"""
	"""

	_db_names = {
		'data': 'Ampel_data',
		'var': 'Ampel_var',
		'ext': 'Ampel_ext'
	}

	# None will be replaced by instance of pymongo.collection.Collection the first time
	# that AmpelDB.get_collection(...) is called for a given collection
	_existing_cols = {
		'data': {
			'main': None,
			'photo': None
		},
		'var': {
			'jobs': None,
			'logs': None,
			'troubles': None
		},
		'ext': {
			'counter': None,
			'runConfig': None,
			'journal': None
		}
	}

	# Existing mongo clients
	_existing_mcs = {}

	# least priviledged role required to write
	_db_config_roles = {
		'data': 'writer',
		'var': 'logger',
		'ext': 'logger'
	}
	
	# least priviledged role required to read
	_db_config_reader_roles = {
		'data': 'logger',
		'var': 'logger',
		'ext': 'logger'
	}

	db_contact = {
		'data': False,	
		'var': False,	
		'ext': False
	}


	@classmethod
	def set_central_db_name(cls, db_name):
		"""
		"""
		cls._db_names['data'] = db_name


	@classmethod
	def get_collection(cls, col_name, mode='w'):
		""" 
		col_name: string or list of strings.
		mode: required permission level, either 'r' for read-only or 'rw' for read-write
		Will return an instance or list of instances of pymongo.collection.Collection.
		If a collection does not exist, it will be created and the 
		proper mongoDB indexes will be set.
		"""

		if type(col_name) in (list, tuple):
			return (cls.get_collection(name) for name in col_name)

		# For now, either 'Ampel' or 'Ampel_logs'
		db_label, db_name = cls._get_associated_db_name(col_name)

		if db_label is None:
			raise ValueError("Unknown collection name: '%s'" % col_name)

		# the collection already exists, no need to create it
		if cls._existing_cols[db_label][col_name] is not None:
			return cls._existing_cols[db_label][col_name]

		# db_label.collection_names() wasn't called yet (we just need to call it once)
		if not cls.db_contact[db_label]:

			mc = cls._get_mongo_client(db_label, mode)
			cls.db_contact[db_label] = True

			for el in mc[db_name].collection_names():

				# Skip unkown existing collections
				if el not in cls._existing_cols[db_label]:
					continue

				# Prior manual customization may have been done
				if cls._existing_cols[db_label][el] is None:
					cls._existing_cols[db_label][el] = mc[db_name][el]
		
		# Ensure indexes for new collection 
		mc = cls._get_mongo_client(db_label, mode)
		col = mc[db_name].get_collection(col_name)
		if 'w' in mode:
			cls.create_indexes(col)

		return col

	
	@classmethod
	def _get_associated_db_name(cls, col_name):
		""" """ 
		for db_label in cls._existing_cols:
			if col_name in cls._existing_cols[db_label].keys():
				return db_label, cls._db_names[db_label]
		return None, None


	@classmethod
	def _get_mongo_client(cls, db_label, mode='w'):
		""" """ 
		from pymongo import MongoClient

		# If a mongoclient does not already exists for this db_label (ex: 'data')
		if not (db_label, mode) in cls._existing_mcs:

			# As of Juli 2018: 'Ampel' -> 'writer' and 'Ampel_logs' -> 'logger'
			if 'w' in mode:
				role = cls._db_config_roles[db_label]
			else:
				role = cls._db_config_reader_roles[db_label]

			cls._existing_mcs[(db_label, mode)] = MongoClient(
				AmpelConfig.get_config('resources.mongo.%s' % role)
			)

		return cls._existing_mcs[(db_label, mode)] 


	@staticmethod
	def create_indexes(col):
		"""
		The method will set indexes for collections with names: 
		'main', 'photo', 'jobs'
		"""

		if col.name == "main":

			col.create_index(
	        	[
	    	        ("tranId", 1), 
	        	    ("alDocType", 1), 
	        	    ("channels", 1)
				]
			)

			# Create sparse runstate index
			col.create_index(
				[("runState", 1)],
				**{ 
					"partialFilterExpression": {
						"alDocType": AlDocTypes.T2RECORD
					}
				}
			)

		elif col.name == "photo":

			col.create_index(
				[("tranId", 1)],
			)

		elif col.name == "jobs":

			# Create sparse index for key hasError
			col.create_index(
				[("hasError", 1)],
				**{ 
					"partialFilterExpression": {
						"hasError": { "$exists": True } 
					}
				}
			)

		elif col.name == "logs":

			col.create_index(
				[("runId", 1)],
			)

			# Create sparse index for key runId
			col.create_index(
				[("tranId", 1)],
				**{ 
					"partialFilterExpression": {
						"tranId": { "$exists": True } 
					}
				}
			)

			# Create sparse index for key runId
			col.create_index(
				[("channel", 1)],
				**{ 
					"partialFilterExpression": {
						"channel": { "$exists": True } 
					}
				}
			)

		elif col.name == "troubles":
			pass
