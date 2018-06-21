#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/db/AmpelDB.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 16.06.2018
# Last Modified Date: 21.06.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from ampel.pipeline.config.AmpelConfig import AmpelConfig
from ampel.flags.AlDocTypes import AlDocTypes

class AmpelDB:
	"""
	"""

	_db_names = {
		'data': 'Ampel_data',
		'logs': 'Ampel_logs',
		'reports': 'Ampel_reports'
	}

	# None will be replaced by instance of pymongo.collection.Collection the first time
	# that AmpelDB.get_collection(...) is called for a given collection
	_existing_cols = {
		'data': {
			'main': None,
			'photo': None
		},
		'logs': {
			'jobs': None,
			'troubles': None
		},
		'reports': {
			'runs': None
		}
	}

	# Existing mongo clients
	_existing_mcs = {
		'data': None,
		'logs': None,
		'reports': None
	}

	_db_config_roles = {
		'data': 'writer',
		'logs': 'logger',
		'reports': 'logger'
	}

	db_contact = {
		'data': False,	
		'logs': False,	
		'reports': False
	}


	@classmethod
	def set_central_db_name(cls, db_name):
		"""
		"""
		cls._db_names['data'] = db_name


	@classmethod
	def get_collection(cls, col_name):
		""" 
		col_name: string or list of strings.
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

			mc = cls._get_mongo_client(db_label)
			cls.db_contact[db_label] = True

			for el in mc[db_name].collection_names():

				# Skip unkown existing collections
				if el not in cls._existing_cols[db_label]:
					continue

				# Prior manual customization may have been done
				if cls._existing_cols[db_label][el] is None:
					cls._existing_cols[db_label][el] = mc[db_name][el]
		
		# Ensure indexes for new collection 
		mc = cls._get_mongo_client(db_label)
		col = mc[db_name].get_collection(col_name)
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
	def _get_mongo_client(cls, db_label):
		""" """ 
		from pymongo import MongoClient

		# If a mongoclient does not already exists for this db_label (ex: 'data')
		if cls._existing_mcs[db_label] is None:

			# As of Juli 2018: 'Ampel' -> 'writer' and 'Ampel_logs' -> 'logger'
			role = cls._db_config_roles[db_label]

			cls._existing_mcs[db_label] = MongoClient(
				AmpelConfig.get_config('resources.mongo.%s' % role)
			)

		return cls._existing_mcs[db_label] 


	@staticmethod
	def create_indexes(col):
		"""
		The method will set indexes for collections with names: 
		'main', 'photo', 'logs'
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
				[
					("runState", 1)
				],
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
				[
					("hasError", 1)
				],
				**{ 
					"partialFilterExpression": {
						"hasError": { "$exists": True } 
					}
				}
			)

		elif col.name == "troubles":
			pass


	@classmethod
	def set_collection(cls, name, value):
		""" 
		"""
		pass
