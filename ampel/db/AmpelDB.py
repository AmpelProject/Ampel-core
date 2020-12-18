#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/db/AmpelDB.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 16.06.2018
# Last Modified Date: 31.01.2021
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import secrets
from collections import defaultdict  # type: ignore[attr-defined]
from typing import Sequence, Dict, List, Any, Union, Optional, TYPE_CHECKING

from pymongo import MongoClient
from pymongo.database import Database
from pymongo.collection import Collection
from pymongo.errors import ConfigurationError

from ampel.type import ChannelId
from ampel.config.AmpelConfig import AmpelConfig
from ampel.base.AmpelBaseModel import AmpelBaseModel
from ampel.model.db.AmpelColModel import AmpelColModel
from ampel.model.db.AmpelDBModel import AmpelDBModel
from ampel.model.db.IndexModel import IndexModel
from ampel.model.db.ShortIndexModel import ShortIndexModel
from ampel.model.db.MongoClientOptionsModel import MongoClientOptionsModel
from ampel.model.db.MongoClientRoleModel import MongoClientRoleModel

if TYPE_CHECKING:
	from ampel.log.AmpelLogger import AmpelLogger

from ampel.abstract.AbsSecretProvider import AbsSecretProvider


intcol = {'t0': 0, 't1': 1, 't2': 2, 'stock': 3}

class AmpelDB(AmpelBaseModel):
	"""
	Ampel stores all information in a dedicated DB.
	This class allows to create or retrieve the underlying database collections.
	"""

	prefix: str = 'Ampel'
	databases: List[AmpelDBModel]
	mongo_uri: str
	mongo_options: MongoClientOptionsModel = MongoClientOptionsModel()
	secrets: Optional[AbsSecretProvider]

	@staticmethod
	def new(config: AmpelConfig, secrets: Optional[AbsSecretProvider] = None) -> 'AmpelDB':
		""" :raises: ValueError in case a required config entry is missing """
		return AmpelDB(
			mongo_uri = config.get('resource.mongo', str, raise_exc=True),
			secrets = secrets,
			**config.get('db', dict, raise_exc=True)
		)


	def __init__(self, **kwargs) -> None:

		super().__init__(**kwargs) # type: ignore[call-arg]

		self.col_config: Dict[str, AmpelColModel] = {
			col.name: col
			for db_config in self.databases
				for col in db_config.collections
		}

		self.mongo_collections: Dict[str, Collection] = {}
		# map role with client
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

		self.databases.append(db_config)

		for col in db_config.collections:
			self.col_config[col.name] = col


	def get_collection(self, col_name: Union[int, str], mode: str = 'w') -> Collection:
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

		db = self._get_mongo_db(
			role=role, db_name=f"{self.prefix}_{db_config.name}"
		)

		if 'w' in mode and col_name not in db.list_collection_names():
			self.mongo_collections[col_name][mode] = self.create_collection(
				role, db.name, self.col_config[col_name]
			)
		else:
			self.mongo_collections[col_name][mode] = db.get_collection(col_name)

		return self.mongo_collections[col_name][mode]


	def _get_mongo_db(self, *, role: str, db_name: str) -> Database:
		if role not in self.mongo_clients:
			key = f'mongo/{role}'
			kwargs = {
				**(self.secrets.get(key, dict).get() if self.secrets else {}),
				**self.mongo_options.dict()
			}
			try:
				self.mongo_clients[role] = MongoClient(self.mongo_uri, **kwargs)
			except ConfigurationError as exc:
				# hint at error source
				raise ConfigurationError(exc.args[0] + f' (from secret {key})', *exc.args[1:])

		return self.mongo_clients[role].get_database(db_name)


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


	def create_collection(self,
		role: str, db_name: str,
		col_config: AmpelColModel, logger: Optional['AmpelLogger'] = None
	) -> Collection:
		"""
		:param resource_name: name of the AmpelConfig resource (resource.mongo) to be fed to MongoClient()
		:param db_name: name of the database to be used/created
		:param col_name: name of the collection to be created
		"""

		if logger is None:
			# Avoid cyclic import error
			from ampel.log.AmpelLogger import AmpelLogger
			logger = AmpelLogger.get_logger()
			logger.info(f"Creating {db_name} -> {col_config.name}")

		db = self._get_mongo_db(role=role, db_name=db_name)

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

					idx_params = idx.dict(exclude_unset=True)
					logger.info(f"  Creating index: {idx_params}")

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
						f"Index creation failed for '{col_config.name}' "
						f"(db: '{db_name}', args: {idx_params})",
						exc_info=e
					)

		return col


	def set_col_index(self,
		role: str, db_name: str, col_config: AmpelColModel,
		force_overwrite: bool = False, logger: Optional['AmpelLogger'] = None
	) -> None:
		"""
		:param force_overwrite: delete index if it already exists.
		This can be useful if you want to change index options (for example: sparse=True/False)
		"""

		if not logger:
			# Avoid cyclic import error
			from ampel.log.AmpelLogger import AmpelLogger
			logger = AmpelLogger.get_logger()

		if not col_config.indexes:
			logger.info(f"No index data configured for collection {col_config.name}")
			return

		db = self._get_mongo_db(role=role, db_name=db_name)

		if col_config.name not in db.list_collection_names():
			self.create_collection(role, db_name, col_config)
			return

		col = self.get_collection(col_config.name)
		col_index_info = col.index_information()
		flat_indexes = []

		for idx in col_config.indexes:

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


	def __repr__(self) -> str:
		return "<AmpelDB>"


	@staticmethod
	def _create_index(
		col: Collection,
		index_data: Union[IndexModel, ShortIndexModel],
		logger: 'AmpelLogger'
	) -> None:

		try:

			idx_params = index_data.dict(exclude_unset=True)
			logger.info(f"  Creating index: {idx_params}")

			if idx_params.get('args'):
				col.create_index(idx_params['index'], **idx_params['args'])
			else:
				col.create_index(idx_params['index'])

		except Exception as e:
			logger.error(
				f"Index creation failed for '{col.name}' (args: {idx_params})",
				exc_info=e
			)


	def drop_all_databases(self):
		for db in self.databases:
			self._get_mongo_db(role=db.role.w, db_name=db.name).client.drop_database(f"{self.prefix}_{db.name}")
		self.mongo_collections.clear()
		self.mongo_clients.clear()


def provision_accounts(ampel_db: AmpelDB, auth: Dict[str, str] = {}) -> Dict[str, Any]:
	"""Create accounts required by the given Ampel configuration."""
	roles = defaultdict(list)
	for db in ampel_db.databases:
		name = f"{ampel_db.prefix}_{db.name}"
		roles[db.role.r].append({"db": name, "role": "read"})
		roles[db.role.w].append({"db": name, "role": "readWrite"})
	users = dict()
	admin = MongoClient(ampel_db.mongo_uri, **auth).get_database("admin")
	tag = secrets.token_hex(8)
	for name, all_roles in roles.items():
		username = f"{name}-{tag}"
		password = secrets.token_hex()
		admin.command("createUser", username, pwd=password, roles=all_roles)
		users[f"mongo/{name}"] = {"username": username, "password": password}
	return users


def revoke_accounts(ampel_db: AmpelDB, auth: Dict[str, str] = {}) -> None:
	"""Delete accounts previously created with "provision"."""
	if ampel_db.secrets is None:
		raise ValueError("No secrets configured")
	admin = MongoClient(ampel_db.mongo_uri, **auth).get_database("admin")
	roles = {role for db in ampel_db.databases for role in db.role.dict().values()}
	for role in roles:
		username = ampel_db.secrets.get(f"mongo/{role}", dict).get()["username"]
		admin.command("dropUser", username)


def list_accounts(ampel_db: AmpelDB, auth: Dict[str, str] = {}) -> Dict[str, Any]:
	"""List configured accounts and roles."""
	admin = MongoClient(ampel_db.mongo_uri, **auth).get_database("admin")
	return admin.command("usersInfo")


def init_db(ampel_db: AmpelDB, auth: Dict[str, str] = {}) -> None:
	"""Initialize Ampel databases and collections."""
	if auth:
		print("="*40)
		print("DANGEROUS THINGS ARE ABOUT TO HAPPEN!")
		print("="*40)
		if input(f"Do you really want to reinitialize databases {[ampel_db.prefix+'_'+db.name for db in ampel_db.databases]} on {ampel_db.mongo_uri}? All data will be lost. Type 'yessir' to proceed: ") == "yessir":
			mc = MongoClient(ampel_db.mongo_uri, **auth)
			for db in ampel_db.databases:
				name = f"{ampel_db.prefix}_{db.name}"
				mc.drop_database(name)
			ampel_db.init_db()
		else:
			print("cancelled")
	else:
		ampel_db.init_db()


def main() -> None:
	import os
	import sys
	from argparse import ArgumentParser
	from getpass import getpass

	import yaml

	from ampel.core import AmpelContext
	from ampel.dev.DictSecretProvider import DictSecretProvider

	parser = ArgumentParser(description="Manage access to Ampel databases")

	def from_env(key) -> Optional[str]:
		if fname := os.environ.get(key + '_FILE', None):
			with open(fname, "r") as f:
				return f.read().rstrip()
		else:
			return os.environ.get(key, None)

	common = ArgumentParser(add_help=False)
	common.add_argument(
		'config_file_path',
		help="Path to an Ampel configuration file"
	)
	common.add_argument(
		'-u',
		'--username',
		type=str,
		default=from_env('MONGO_ROOT_USERNAME'),
		help="Mongo admin username",
	)
	common.add_argument(
		'-p',
		'--password',
		type=str,
		default=from_env('MONGO_ROOT_PASSWORD'),
		help="Mongo admin password",
	)

	subparsers = parser.add_subparsers(dest="command")
	subparsers.required = True

	def add_command(func, name=None):
		p = subparsers.add_parser(
			func.__name__ if not name else name,
			help=func.__doc__,
			parents=[common],
		)
		p.set_defaults(command=func)
		return p

	add_command(provision_accounts, "provision")

	p = add_command(revoke_accounts, "revoke")
	p.add_argument(
		'secrets',
		type=DictSecretProvider.load,
		default=None,
		help="Path to yaml file containing the accounts entries to be revoked",
	)

	add_command(list_accounts, "list")

	p = add_command(init_db, "init")
	p.add_argument(
		'--secrets',
		type=DictSecretProvider.load,
		default=None,
		help="Path to yaml file with auth secrets",
	)

	args = parser.parse_args()
	if args.username is not None:
		if args.password is None:
			args.password = getpass(prompt=f"Password for Mongo user {args.username}: ")
		auth = {
			"username": args.username,
			"password": args.password,
		}
	else:
		auth = {}

	ctx = AmpelContext.load(args.config_file_path, secrets=getattr(args, "secrets", None))

	if (ret := args.command(ctx.db, auth)):
		yaml.dump(ret, sys.stdout, sort_keys=False)
