#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/db/AmpelDB.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 16.06.2018
# Last Modified Date: 16.02.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import collections
from logging import Logger
from pymongo import MongoClient
from pymongo.database import Database
from pymongo.collection import Collection
from typing import Sequence, Dict, List, Any

from ampel.types import ChannelId
from ampel.config.AmpelConfig import AmpelConfig
from ampel.model.AmpelStrictModel import AmpelStrictModel
from ampel.model.db.AmpelColModel import AmpelColModel
from ampel.model.db.AmpelDBModel import AmpelDBModel
from ampel.model.db.IndexModel import IndexModel
from ampel.model.db.MongoClientRoleModel import MongoClientRoleModel


class AmpelDB(AmpelStrictModel):

	prefix: str = 'Ampel'
	dbs_config: List[AmpelDBModel]
	mongo_resources: Dict[str, Any]


	@staticmethod
	def from_config(ampel_config: AmpelConfig) -> 'AmpelDB':
		"""
		:raises: ValueError in case a required config entry is missing
		"""
		return AmpelDB(
			mongo_resources = ampel_config.get('resource.mongo', dict, True),
			dbs_config = ampel_config.get(
				'db.databases', collections.abc.Sequence, True
			), # type: ignore
			prefix = ampel_config.get('db.prefix', str, True)
		)


	def __init__(self, **kwargs):

		super().__init__(**kwargs)

		self.col_config: Dict[str, AmpelColModel] = {
			col.name: col
			for db_config in self.dbs_config
				for col in db_config.collections
		}

		self.mongo_collections: Dict[str, Collection] = {}
		self.mongo_clients: Dict[str, MongoClient] = {}


	def enable_rejected_collections(self, channel_names: Sequence[ChannelId]) -> None:
		"""
		Makes rejected collections (DB: Ampel_rej, collection: <channel_name>)
		available through standard method call AmpelDB.get_collection(channel_name)
		:param channel_names: list of channel names
		"""
		db_config = AmpelDBModel(
			name = 'rej',
			collections = [
				AmpelColModel(
					name=chan_name if isinstance(chan_name, str) else str(chan_name)
				) for chan_name in channel_names
			],
			role = MongoClientRoleModel(r='logger', w='logger')
		)

		self.dbs_config.append(db_config)

		for col in db_config.collections:
			self.col_config[col.name] = col


	def get_collection(self, col_name: str, mode: str = 'w') -> Collection:
		"""
		:param mode: required permission level, either 'r' for read-only or 'rw' for read-write
		If a collection does not exist, it will be created and the proper mongoDB indexes will be set.
		"""

		if col_name in self.mongo_collections:
			if mode in self.mongo_collections[col_name]:
				return self.mongo_collections[col_name][mode]
		else:
			if col_name not in self.col_config:
				raise ValueError(f"Unknown collection: '{col_name}'")
			self.mongo_collections[col_name] = {}

		db_config = self._get_db_config(col_name)
		resource_name = db_config.role.dict()[mode]

		db = self._get_mongo_db(
			resource_name,
			f"{self.prefix}_{db_config.name}"
		)

		if 'w' in mode and col_name not in db.list_collection_names():
			self.mongo_collections[col_name][mode] = self.create_collection(
				resource_name, db.name, self.col_config[col_name]
			)
		else:
			self.mongo_collections[col_name][mode] = db.get_collection(col_name)

		return self.mongo_collections[col_name][mode]


	def _get_mongo_db(self, resource_name: str, db_name: str) -> Database:
		""" """
		if resource_name not in self.mongo_clients:
			self.mongo_clients[resource_name] = MongoClient(
				self.mongo_resources[resource_name]
			)

		return self.mongo_clients[resource_name].get_database(db_name)


	def _get_db_config(self, col_name: str) -> AmpelDBModel:
		""" """
		return next(
			filter(
				lambda x: self.col_config[col_name] in x.collections,
				self.dbs_config
			)
		)


	def init_db(self) -> None:
		"""
		"""
		for db_config in self.dbs_config:
			for col_config in db_config.collections:
				self.create_collection(
					db_config.role.dict()['w'],
					f"{self.prefix}_{db_config.name}",
					col_config
				)


	def create_collection(self,
		resource_name: str, db_name: str, col_config: AmpelColModel, logger: Logger = None
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


	def set_col_index(self,
		resource_name: str, db_name: str, col_config: AmpelColModel,
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
			logger.info(f"No index data configured for collection {col_config.name}")
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
					logger.info("  Deleting existing index: {idx_id}")
					col.drop_index(idx_id)
				else:
					logger.info(f"  Skipping already existing index: {idx_id}")
					continue

			self._create_index(col, idx, logger)

		for k in col_index_info:
			if k not in flat_indexes and k != "_id_":
				logger.info(f"  Removing index {k}")
				col.drop_index(k)


	def __repr__(self) -> str: # type: ignore
		return "<AmpelDB>"


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
