#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/db/AmpelDB.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 16.06.2018
# Last Modified Date: 04.11.2019
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from logging import Logger
from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.database import Database
from typing import Union, Tuple, Sequence
from ampel.config.AmpelConfig import AmpelConfig
from ampel.model.db.AmpelColModel import AmpelColModel
from ampel.model.db.AmpelDBModel import AmpelDBModel
from ampel.model.db.IndexModel import IndexModel


class AmpelDB:
	"""
	"""

	def __init__(self, ampel_config: AmpelConfig, prefix_override=None):
		"""
		:param prefix_override: Convenience parameter that overrides 
		the db prefix name defined in AmpelConfig (db.prefix)
		"""
		self.ampel_config = ampel_config
		self._db_prefix = ampel_config.get('db.prefix') if not prefix_override else prefix_override
		self._mongo_resources = ampel_config.get('resource.mongo')

		if self._mongo_resources is None:
			raise ValueError("Mongo resources not set in provided ampel config")

		self.dbs_config = [
			AmpelDBModel(**el) for el in ampel_config.get('db.databases')
		]

		self._col_config = {
			col.name: col
			for db_config in self.dbs_config
				for col in db_config.collections
		}

		self._mongo_collections = {}
		self._mongo_clients = {}


	def enable_rejected_collections(self, channel_names: Sequence[str]) -> None:
		"""
		Makes rejected collections (DB: Ampel_rej, collection: <channel_name>)
		available through standard method call AmpelDB.get_collection(channel_name)
		:param channel_names: list of channel names
		"""
		db_config = AmpelDBModel(
			name='rej',
			collections=[
				{'name': chan_name} for chan_name in channel_names
			],
			role={'r': 'logger', 'w': 'logger'}
		)

		self.dbs_config.append(db_config)

		for col in db_config.collections:
			self._col_config[col.name] = col
			

	def get_collection(self, col_name: str, mode: str = 'w') -> Union[Collection, Tuple[Collection]]:
		""" 
		If a collection does not exist, it will be created and the 
		proper mongoDB indexes will be set.

		:param str col_name: string or list of strings.
		:param str mode: required permission level, either 'r' for read-only or 'rw' for read-write
		:returns: instance or list of instances of pymongo.collection.Collection.
		"""

		# Convenience
		if isinstance(col_name, (list, tuple)):
			return (self.get_collection(name) for name in col_name)

		if col_name in self._mongo_collections:
			if mode in self._mongo_collections[col_name]:
				return self._mongo_collections[col_name][mode]
		else:
			if col_name not in self._col_config:
				raise ValueError(f"Unknown collection: '{col_name}'")
			self._mongo_collections[col_name] = {}

		db_config = self._get_db_config(col_name)
		resource_name = db_config.role.dict()[mode]
			
		db = self._get_mongo_db(
			resource_name, 
			f"{self._db_prefix}_{db_config.name}"
		)

		if 'w' in mode and col_name not in db.list_collection_names():
			self._mongo_collections[col_name][mode] = self.create_collection(
				resource_name, db.name, self._col_config[col_name]
			)
		else:
			self._mongo_collections[col_name][mode] = db.get_collection(col_name)

		return self._mongo_collections[col_name][mode]


	def _get_mongo_db(self, resource_name: str, db_name: str) -> Database:
		""" """
		if resource_name not in self._mongo_clients:
			self._mongo_clients[resource_name] = MongoClient(
				self._mongo_resources[resource_name]
			)

		return self._mongo_clients[resource_name].get_database(db_name)


	def _get_db_config(self, col_name: str) -> AmpelDBModel:
		""" """
		return next(
			filter(
				lambda x: self._col_config[col_name] in x.collections,
				self.dbs_config
			)
		)


	def init_db(self):
		"""
		"""
		for db_config in self.dbs_config:
			for col_config in db_config.collections:
				self.create_collection(
					db_config.role.dict()['w'],
					f"{self._db_prefix}_{db_config.name}",
					col_config
				)

	
	def create_collection(
		self, resource_name: str, db_name: str, col_config: AmpelColModel, logger: Logger = None
	) -> Collection:
		""" 
		:param resource_name: name of the AmpelConfig resource (resource.mongo) to be fed to MongoClient()
		:param db_name: name of the database to be used/created
		:param col_name: name of the collection to be created
		"""

		if not logger:
			# Avoid cyclic import error
			from ampel.logging.AmpelLogger import AmpelLogger
			logger = AmpelLogger.get_logger()
			logger.info("Creating %s -> %s", db_name, col_config.name)

		db = self._get_mongo_db(resource_name, db_name)

		# Create collection with custom args
		if col_config.args:
			col = db.create_collection(
				col_config.name, **col_config.args
			)
		else:
			col = db.create_collection(col_config.name)

		if col_config.indexes:

			for idx in col_config.indexes:

				try:

					idx_params = idx.dict(skip_defaults=True)
					logger.info("  Creating index: %s", idx_params)

					if idx_params.get('args'):
						col.create_index(
							idx_params['index'], **idx_params['args']
						)
					else:
						col.create_index(
							idx_params['index']
						)
									
				except Exception as e:
					logger.error(
						"Index creation failed for '%s' (db: '%s', args: %s)", 
						col_config.name, db_name, idx_params, 
						exc_info=e
					)

		return col


	def set_col_index( 
		self, resource_name: str, db_name: str, col_config: AmpelColModel, 
		force_overwrite: bool = False, logger: Logger = None
	) -> None:
		"""
		:param force_overwrite: delete index if it already exists. 
		This can be useful if you want to change index options (for example: sparse=True/False)
		"""

		if not logger:
			# Avoid cyclic import error
			from ampel.logging.AmpelLogger import AmpelLogger
			logger = AmpelLogger.get_logger()

		if not col_config.indexes:
			logger.info(
				"No index data configured for collection " +
				col_config.name
			)
			return

		db = self._get_mongo_db(resource_name, db_name)

		if col_config.name not in db.list_collection_names():
			self.create_collection(resource_name, db_name, col_config)
			return

		col = self.get_collection(col_config.name)
		col_index_info = col.index_information()
		flat_indexes = []

		for idx in col_config.indexes:

			idx_id = idx.get_id()
			flat_indexes.append(idx_id)

			if idx_id in col_index_info:
				if force_overwrite:
					logger.info("  Deleting existing index: "+idx_id)
					col.drop_index(idx_id)
				else:
					logger.info("  Skipping already existing index: "+idx_id)
					continue

			self._create_index(col, idx, logger)

		for k in col_index_info:
			if k not in flat_indexes and k != "_id_":
				logger.info("  Removing index "+k)
				col.drop_index(k)

								
	@staticmethod
	def _create_index(col: Collection, index_data: IndexModel, logger: Logger) -> None:
		"""
		"""

		try:

			idx_params = index_data.dict(skip_defaults=True)
			logger.info("  Creating index: %s", idx_params)

			if idx_params.get('args'):
				col.create_index(
					idx_params['index'], 
					**idx_params['args']
				)
			else:
				col.create_index(
					idx_params['index']
				)

		except Exception as e:
			logger.error(
				"Index creation failed for '%s' (args: %s)", 
				col.name, idx_params, exc_info=e
			)
