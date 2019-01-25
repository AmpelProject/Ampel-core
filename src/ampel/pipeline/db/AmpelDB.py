#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/pipeline/db/AmpelDB.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 16.06.2018
# Last Modified Date: 26.11.2018
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from ampel.pipeline.config.AmpelConfig import AmpelConfig
from ampel.core.flags.AlDocType import AlDocType

class AmpelDB:
	"""
	"""

	_db_prefix = "Ampel"

	# Existing mongo clients
	_existing_mcs = {}

	# 'col' None will be replaced by instance of pymongo.collection.Collection 
	# the first time AmpelDB.get_collection(...) is called for a given collection
	_ampel_cols = {
		'tran': {
			'dbLabel': 'data',
			'dbPrefix': _db_prefix,
			'col': None
		},
		'photo': {
			'dbLabel': 'data',
			'dbPrefix': _db_prefix,
			'col': None
		},
		'blend': {
			'dbLabel': 'data',
			'dbPrefix': _db_prefix,
			'col': None
		},
		'logs': {
			'dbLabel': 'var',
			'dbPrefix': _db_prefix,
			'col': None
		},
		'events': {
			'dbLabel': 'var',
			'dbPrefix': _db_prefix,
			'col': None
		},
		'beacon': {
			'dbLabel': 'var',
			'dbPrefix': _db_prefix,
			'col': None
		},
		'troubles': {
			'dbLabel': 'var',
			'dbPrefix': _db_prefix,
			'col': None
		},
		'counter': {
			'dbLabel': 'ext',
			'dbPrefix': _db_prefix,
			'col': None
		},
		'runConfig': {
			'dbLabel': 'ext',
			'dbPrefix': _db_prefix,
			'col': None
		},
		'journal': {
			'dbLabel': 'ext',
			'dbPrefix': _db_prefix,
			'col': None
		}
	}

	# least priviledged role required to read or write
	_db_roles = {
		'data': {
			'r': 'logger',
			'w': 'writer'
		},
		'var': {
			'r': 'logger',
			'w': 'logger'
		},
		'ext': {
			'r': 'logger',
			'w': 'logger'
		}
	}

	@classmethod
	def set_db_prefix(cls, prefix):
		"""
		:returns: None
		"""
		cls._db_name_prefix = prefix
		for d in cls._ampel_cols.values():
			d['dbPrefix'] = prefix
			d['col'] = None

	@classmethod
	def reset(cls):
		cls._existing_mcs.clear()
		for col_config in cls._ampel_cols.values():
			col_config['col'] = None

	@classmethod
	def enable_rejected_collections(cls, channel_names):
		"""
		Makes rejected collections (DB: Ampel_rej, colllection: channel_name)
		available through standard method call AmpelDB.get_collection(channel_name)

		:param list(str) channel_names: list of channel names
		:returns: None
		"""

		cls._db_roles['rej'] = {'r': 'logger', 'w': 'logger'}
		for chan_name in channel_names:
			cls._ampel_cols[chan_name] = {
				'dbLabel': 'rej',
				'dbPrefix': cls._db_prefix,
				'col': None
			}


	@classmethod
	def get_collection(cls, col_name, mode='w'):
		""" 
		If a collection does not exist, it will be created and the 
		proper mongoDB indexes will be set.

		:param str col_name: string or list of strings.
		:param str mode: required permission level, either 'r' for read-only or 'rw' for read-write
		:returns: instance or list of instances of pymongo.collection.Collection.
		"""

		# Convenience
		if type(col_name) in (list, tuple):
			return (cls.get_collection(name) for name in col_name)

		if col_name not in cls._ampel_cols:
			raise ValueError("Unknown collection: '%s'" % col_name)

		# Shortcut
		col_config = cls._ampel_cols[col_name]

		# the collection already exists, no need to create it
		if col_config['col'] is not None:
			return col_config['col']

		# db_label.collection_names() wasn't called yet for this col
		mc = cls._get_mongo_client(col_config['dbLabel'], mode)
		db = mc[col_config['dbPrefix'] + "_" + col_config['dbLabel']]

		if 'w' in mode:
			if col_name not in db.collection_names():
				try:
					cls.create_indexes(db, col_name)
				except Exception:
					import logging
					logging.error("Col index creation failed", exc_info=True)

		col_config['col'] = db[col_name]
		return db[col_name]

	
	@classmethod
	def _get_mongo_client(cls, db_label, mode='w'):
		"""
		:param str db_label: db label (data/var/ext) 
		:param str mode: access mode "w" or "r"
		:returns: MongoClient instance
		""" 
		from pymongo import MongoClient

		# example: 'logger' or 'writer'
		role = cls._db_roles[db_label][mode]

		# If a mongoclient does not already exists for this db_label (ex: 'data')
		if not role in cls._existing_mcs:

			# As of Juli 2018: 'Ampel_data' -> 'writer' and 'Ampel_logs' -> 'logger'
			cls._existing_mcs[role] = MongoClient(
				AmpelConfig.get_config('resources.mongo.%s' % role)
			)

		return cls._existing_mcs[role] 


	@staticmethod
	def create_indexes(db, col_name):
		"""
		The method will set indexes for collections with names: 
		'tran', 'photo', 'blend', 'events', 'logs', 'troubles', ...

		:returns: None
		"""
		import logging
		logging.info(
			"Creating index for collection '%s' (db: '%s')" % 
			(col_name, db.name)
		)

		if col_name == "tran":

			# For various indexed queries and live auto-complete *covered* queries
			db[col_name].create_index(
				[
					('_id', 1),
					('channels', 1)
				],
				unique = True
			)

		elif col_name == "photo":

			db[col_name].create_index(
				[('tranId', 1)]
			)

		elif col_name == "blend":

			db[col_name].create_index(
				[
					('tranId', 1), 
					('alDocType', 1), 
					('channels', 1)
				]
			)

			# Create sparse runstate index
			db[col_name].create_index(
				[('runState', 1)],
				partialFilterExpression = {
					'alDocType': AlDocType.T2RECORD
				}
			)

		elif col_name == "logs":

			db.create_collection(
				'logs',
				storageEngine={
					'wiredTiger': {
						'configString': 'block_compressor=zlib'
					}
				}
			)

			db[col_name].create_index(
				[('runId', 1)],
			)

			# Create sparse index for key tranId
			# Note: this is more of a convenience index. The transient doc
			# contains a list of runIds which could greatly reduce the 
			# number of log entries to scan. Matching tranId in a such 
			# reduced scope should be achievable without waiting ages.
			# On the other side, there should not be a lot of log entries 
			# associated with a tranId, so that the indexing perf penalty 
			# should not be an issue. Time will tell...
			db[col_name].create_index(
				[('tranId', 1)],
				partialFilterExpression = {
					'tranId': {'$exists': True} 
				}
			)

			# Create sparse index for key channels
			db[col_name].create_index(
				[('channels', 1)],
				partialFilterExpression = {
					'channels': {'$exists': True} 
				}
			)

		elif col_name == "events":
			pass

			# Create sparse index for key hasError
			# db[col_name].create_index(
			#	[('hasError', 1)],
			#	partialFilterExpression = {
			#		'hasError': { '$exists': True } 
			#	}
			#)

		# Rejected logs collections
		elif (
			col_name == "rejected" or
			(col_name in AmpelDB._ampel_cols and AmpelDB._ampel_cols[col_name]['dbLabel'] == 'rej')
		):

			db.create_collection(
				col_name,
				storageEngine={
					'wiredTiger': {
						'configString': 'block_compressor=zlib'
					}
				}
			)

			# Create sparse index for key runId
			db[col_name].create_index([('tranId', 1)])
