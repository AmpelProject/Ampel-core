#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/core/AmpelDB.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 16.06.2018
# Last Modified Date: 27.08.2022
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import collections.abc
from collections.abc import Sequence
from contextlib import suppress
from functools import cached_property
from typing import Any, Literal

from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.database import Database
from pymongo.errors import (
	CollectionInvalid,
	ConfigurationError,
	DuplicateKeyError,
	OperationFailure,
)
from pymongo.read_preferences import SecondaryPreferred

from ampel.base.AmpelUnit import AmpelUnit
from ampel.config.AmpelConfig import AmpelConfig
from ampel.log.AmpelLogger import AmpelLogger
from ampel.mongo.model.AmpelColModel import AmpelColModel
from ampel.mongo.model.AmpelDBModel import AmpelDBModel
from ampel.mongo.model.IndexModel import IndexModel
from ampel.mongo.model.MongoClientOptionsModel import MongoClientOptionsModel
from ampel.mongo.model.MongoClientRoleModel import MongoClientRoleModel
from ampel.mongo.model.ShortIndexModel import ShortIndexModel
from ampel.mongo.utils import get_ids
from ampel.mongo.view.AbsMongoView import AbsMongoView
from ampel.mongo.view.MongoAndView import MongoAndView
from ampel.mongo.view.MongoOneView import MongoOneView
from ampel.mongo.view.MongoOrView import MongoOrView
from ampel.secret.AmpelVault import AmpelVault
from ampel.types import ChannelId
from ampel.util.collections import try_reduce

intcol = {'t0': 0, 't1': 1, 't2': 2, 't3': 3, 'stock': 4}


class UnknownDatabase(Exception):
	pass


class AmpelDB(AmpelUnit):
	"""
	Ampel stores information in a dedicated DB.
	This class allows to create or retrieve the underlying database collections.
	"""

	prefix: str = 'Ampel'
	databases: Sequence[AmpelDBModel]
	mongo_uri: str
	mongo_options: MongoClientOptionsModel = MongoClientOptionsModel()
	vault: None | AmpelVault
	require_exists: bool = False
	one_db: bool = False
	#: Route reads to secondaries of a replica set. This can increase
	#: performance at the expense of consistency
	read_from_secondary: bool = True
	#: Ignore secondary if it is more than max_staleness seconds behind the
	#: primary
	max_staleness: int = 300


	@classmethod
	def new(cls,
		config: AmpelConfig,
		vault: None | AmpelVault = None,
		require_exists: bool | str = False,
		one_db: bool | Literal['auto'] = False
	) -> 'AmpelDB':
		"""
		:param require_exists:
		- False: AmpelDB instantiation will succeed even if underlying database(s) do not exist
		- True: AmpelDB instantiation will fail unless underlying databases exist
		- str: same as True but provided value overrides prefix value otherwise loaded from AmpelConfig
		:param one_db: whether to create or load a single database rather than having
		collections split in in three databases (data, var, ext).
		If 'auto' is set (for which require_exists must be True), then AmpelDB will try
		to load a database in single-collection mode and try again in multi-collections mode if it fails.
		Note that ampel 'jobs' usually operate with one_db=True.
		:raises: ValueError in case a required config entry is missing
		"""
		if one_db == 'auto':
			if require_exists is False:
				raise ValueError("Option 'one_db' cannot be set to 'auto' if require_exists is False")
			try:
				return cls._new(config, vault, require_exists, True)
			except UnknownDatabase:
				return cls._new(config, vault, require_exists, False)
		return cls._new(config, vault, require_exists, one_db)


	@classmethod
	def _new(cls,
		config: AmpelConfig,
		vault: None | AmpelVault = None,
		require_exists: bool | str = False,
		one_db: bool = False,
	) -> 'AmpelDB':
		""" :raises: ValueError in case a required config entry is missing """
		db_config = config.get('mongo', dict, raise_exc=True)
		if isinstance(require_exists, str):
			dict.__setitem__(db_config, 'prefix', require_exists)
			require_exists = True
		return cls(
			mongo_uri = config.get('resource.mongo', str, raise_exc=True),
			vault = vault,
			require_exists = require_exists,
			one_db = one_db,
			**db_config
		)


	def __init__(self, **kwargs) -> None:

		if 'ingest' in kwargs:
			kwargs.pop('ingest')

		super().__init__(**kwargs)

		self.col_config: dict[str, AmpelColModel] = {
			col.name: col
			for db_config in self.databases
			for col in db_config.collections
		}

		self.mongo_collections: dict[str, dict[str, Collection]] = {}
		self.mongo_clients: dict[str, MongoClient] = {} # map role with client

		if (
			self.require_exists
			and not all(
				self._get_pymongo_db(db.name, role=db.role.w).list_collection_names()
				for db in self.databases
			)
		):
			raise UnknownDatabase(f"Database(s) with prefix {self.prefix} do not exist")

	def close(self) -> None:
		for mc in self.mongo_clients.values():
			mc.close()
		self.mongo_collections.clear()
		self.mongo_clients.clear()
		# deleting the attribute resets cached_property
		for attr in ("col_trace_ids", "col_conf_ids", "trace_ids", "conf_ids"):
			with suppress(AttributeError):
				delattr(self, attr)

	@cached_property
	def col_trace_ids(self) -> Collection:
		return self.get_collection('trace')
	
	@cached_property
	def col_conf_ids(self) -> Collection:
		return self.get_collection('conf')

	@cached_property
	def trace_ids(self) -> set[int]:
		return get_ids(self.col_trace_ids)
	
	@cached_property
	def conf_ids(self) -> set[int]:
		return get_ids(self.col_conf_ids)


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
					name = chan_name if isinstance(chan_name, str) else str(chan_name)
				)
				for chan_name in channel_names
			],
			role = MongoClientRoleModel(r='logger', w='logger')
		)

		self.databases = [*self.databases, db_config]

		for col in db_config.collections:
			self.col_config[col.name] = col


	def get_collection(self, col_name: int | str, mode: str = 'w') -> Collection:
		"""
		:param mode: required permission level, either 'r' for read-only or 'rw' for read-write
		If a collection does not exist, it will be created and the proper mongoDB indexes will be set.
		"""

		if isinstance(col_name, int):
			col_name = str(col_name)

		if col_name in self.mongo_collections:
			if mode in self.mongo_collections[col_name]:
				return self.mongo_collections[col_name][mode]
		else:
			if col_name not in self.col_config:
				raise ValueError(f"Unknown collection: '{col_name}'")
			self.mongo_collections[col_name] = {}

		db_config = self._get_db_config(col_name)
		role = db_config.role.dict()[mode]
		db = self._get_pymongo_db(db_config.name, role=role)

		if 'w' in mode and col_name not in db.list_collection_names():
			try:
				self.create_ampel_collection(
					self.col_config[col_name], db_config.name, role
				)
			except CollectionInvalid:
				# raised when collection was created concurrently
				...
			except OperationFailure as exc:
				# also raised when collection was created concurrently
				if exc.code != 48: # NamespaceExists
					raise
		self.mongo_collections[col_name][mode] = db.get_collection(
			col_name,
			read_preference=SecondaryPreferred(max_staleness=self.max_staleness) if self.read_from_secondary else None
		)

		return self.mongo_collections[col_name][mode]


	def _get_pymongo_db(self, db_name: str, *, role: str) -> Database:
		"""
		:param db_name: without prefix
		"""

		if role not in self.mongo_clients:

			kwargs = self.mongo_options.dict()
			key = f'mongo/{role}'
			if self.vault and (ns := self.vault.get_named_secret(key, dict)):
				kwargs |= ns.get()

			try:
				self.mongo_clients[role] = MongoClient(self.mongo_uri, **kwargs)
			except ConfigurationError as exc:
				# hint at error source
				raise ConfigurationError(exc.args[0] + f' (from secret {key})', *exc.args[1:]) from exc

		return self.mongo_clients[role].get_database(
			f"{self.prefix}_{db_name}" if (db_name and not self.one_db) else self.prefix
		)


	def _get_db_config(self, col_name: str) -> AmpelDBModel:
		return next(
			filter(
				lambda x: self.col_config[col_name] in x.collections,
				self.databases
			)
		)


	def init_db(self) -> None:

		for db_config in self.databases:
			for col_config in db_config.collections:
				self.get_collection(col_config.name)

		self.get_collection('trace')
		self.get_collection('conf')


	def create_ampel_collection(self,
		col_config: AmpelColModel,
		db_name: str,
		role: str,
		logger: 'None | AmpelLogger' = None
	) -> Collection:
		"""
		:param resource_name: name of the AmpelConfig resource (resource.mongo) to be fed to MongoClient()
		:param db_name: name of the database to be used/created
		:param col_name: name of the collection to be created
		"""

		db = self._get_pymongo_db(db_name, role=role)

		if logger is None:
			# Avoid cyclic import error
			from ampel.log.AmpelLogger import AmpelLogger
			logger = AmpelLogger.get_logger()

		try:
			dbinfo = "[" + str(try_reduce(list(db.client.nodes))) + "]"
		except Exception:
			dbinfo = ""

		logger.info(f"Creating {db.name} -> {col_config.name} {dbinfo}")
		col = db.create_collection(col_config.name, **col_config.args)

		"""
		if col_config.name not in db.list_collection_names():
			# Create collection with custom args
			col = db.create_collection(col_config.name, **col_config.args)
		else:
			col = db.get_collection(col_config.name)
		"""

		if col_config.indexes:
			for idx in col_config.indexes:
				self._create_index(col, idx, logger)
	
		return col


	def set_col_index(self,
		col: Collection,
		config: AmpelColModel,
		logger: 'AmpelLogger',
		force_overwrite: bool = False
	) -> None:
		"""
		:param force_overwrite: delete index if it already exists.
		This can be useful if you want to change index options (for example: sparse=True/False)
		"""

		if not config.indexes:
			logger.info(f"No index data configured for collection {config.name}")
			return

		col_index_info = col.index_information()
		flat_indexes = []

		for idx in config.indexes:

			idx_id = idx.get_id()
			flat_indexes.append(idx_id)

			if idx_id in col_index_info:
				if force_overwrite:
					logger.info(f"  Deleting existing index: {idx_id}")
					col.drop_index(idx_id)
				else:
					logger.info(f"  Skipping already existing index: {idx_id}")
					continue

			self._create_index(col, idx, logger)

		for k in col_index_info:
			if k not in flat_indexes and k != "_id_":
				logger.info(f"  Removing index {k}")
				col.drop_index(k)


	def _create_index(self,
		col: Collection,
		index_data: IndexModel | ShortIndexModel,
		logger: 'AmpelLogger'
	) -> None:
		idx_params = index_data.dict(exclude_unset=True)
		logger.info(f"  Creating index for {col.database.name}.{col.name}: {idx_params}")
		col.create_index(idx_params['index'], **idx_params.get('args', {}))


	def create_one_view(self,
		channel: ChannelId,
		logger: 'None | AmpelLogger' = None,
		force: bool = False
	) -> None:
		self.create_view(
			MongoOneView(channel=channel), str(channel),
			logger, force
		)


	def create_or_view(self,
		channels: Sequence[ChannelId],
		logger: 'None | AmpelLogger' = None,
		force: bool = False
	) -> None:

		if not isinstance(channels, collections.abc.Sequence) or len(channels) == 1:
			raise ValueError("Incorrect argument")

		self.create_view(
			MongoOrView(channel=channels),
			"_OR_".join(map(str, channels)),
			logger, force
		)


	def create_and_view(self,
		channels: Sequence[ChannelId],
		logger: 'None | AmpelLogger' = None,
		force: bool = False
	) -> None:

		if not isinstance(channels, collections.abc.Sequence) or len(channels) == 1:
			raise ValueError("Incorrect argument")

		self.create_view(
			MongoAndView(channel=channels),
			"_AND_".join(map(str, channels)),
			logger, force
		)


	def create_view(self,
		view: AbsMongoView,
		col_prefix: str,
		logger: 'None | AmpelLogger' = None,
		force: bool = False
	) -> None:

		db_conf = self._get_db_config("stock")
		db = self._get_pymongo_db(db_conf.name, role=db_conf.role.w)
		if force:
			col_names = db.list_collection_names()

		for el in ("stock", "t0", "t1", "t2", "t3"):

			agg = getattr(view, el)()

			if force and f'{col_prefix}_{el}' in col_names:
				if logger:
					logger.info(f"Discarding previous view {col_prefix}_{el}")
				db.drop_collection(f"{col_prefix}_{el}")

			if logger and logger.verbose > 1:
				from ampel.util.pretty import prettyjson
				logger.info(f"Collection {el} aggregation pipeline:")
				for line in prettyjson(agg).split("\n"):
					logger.info(line)
				logger.info("-"*80)

			db.create_collection(f'{col_prefix}_{el}', viewOn=el, pipeline=agg)


	def delete_one_view(self, channel: ChannelId, logger: 'None | AmpelLogger' = None) -> None:
		self.delete_view(str(channel), logger)


	def delete_or_view(self, channels: Sequence[ChannelId], logger: 'None | AmpelLogger' = None) -> None:
		if not isinstance(channels, collections.abc.Sequence) or len(channels) == 1:
			raise ValueError("Incorrect argument")
		self.delete_view("_OR_".join(map(str, channels)), logger)


	def delete_and_view(self, channels: Sequence[ChannelId], logger: 'None | AmpelLogger' = None) -> None:
		if not isinstance(channels, collections.abc.Sequence) or len(channels) == 1:
			raise ValueError("Incorrect argument")
		self.delete_view("_AND_".join(map(str, channels)), logger)


	def delete_view(self, view_prefix: str, logger: 'None | AmpelLogger' = None) -> None:

		db_conf = self._get_db_config("stock")
		db = self._get_pymongo_db(db_conf.name, role=db_conf.role.w)
		for el in ("stock", "t0", "t1", "t2", "t3"):
			db.drop_collection(f'{view_prefix}_{el}')


	def __repr__(self) -> str:
		return "<AmpelDB>"


	def drop_all_databases(self) -> None:
		for db in self.databases:
			pym_db = self._get_pymongo_db(db.name, role=db.role.w)
			pym_db.client.drop_database(pym_db.name)
		self.close()


	def add_trace_id(self, trace_id: int, arg: dict[str, Any]) -> None:

		# Save trace id to external collection
		if trace_id not in self.trace_ids:

			# Suppress duplicate key error on purpose because update_one/upsert does not maintain dict key order
			with suppress(DuplicateKeyError):
				self.col_trace_ids.insert_one({'_id': trace_id} | arg)

		self.trace_ids.add(trace_id)


	def add_conf_id(self, conf_id: int, arg: dict[str, Any]) -> None:

		# Save conf id to external collection
		if conf_id not in self.conf_ids:

			# Suppress duplicate key error on purpose because update_one/upsert does not maintain dict key order
			with suppress(DuplicateKeyError):
				self.col_conf_ids.insert_one({'_id': conf_id} | arg)

		self.conf_ids.add(conf_id)
