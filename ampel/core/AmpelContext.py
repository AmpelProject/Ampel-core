#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/core/AmpelContext.py
# License:             BSD-3-Clause
# Author:              valery brinnel <firstname.lastname@gmail.com>
# Date:                18.02.2020
# Last Modified Date:  11.11.2025
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

import uuid
from collections.abc import Iterable
from typing import TYPE_CHECKING, Any
from typing_extensions import Self

from ampel.base.AuxUnitRegister import AuxUnitRegister
from ampel.config.AmpelConfig import AmpelConfig
from ampel.secret.AmpelVault import AmpelVault

# Avoid cyclic import issues
if TYPE_CHECKING:
	from ampel.core.AmpelDB import AmpelDB
	from ampel.core.UnitLoader import UnitLoader


class AmpelContext:
	"""
	Execution context for Ampel processes.

	The :class:`AmpelContext` bundles together configuration, database
	connections, unit loading, and shared resources. It is typically
	created by Ampel and passed to processor units so they can access
	system-wide services.

	This class also manages runtime aliases, configuration IDs, and
	provides utilities for generating unique run identifiers. Future
	versions may support multiple :class:`~ampel.config.AmpelConfig.AmpelConfig`
	and :class:`~ampel.core.AmpelDB.AmpelDB` instances.

	.. note::
	   Auxiliary units are auto-registered globally via ``AuxUnitRegister``.
	   Run-time aliases must begin with ``%%``.
	   Convenience constructors (:meth:`load`, :meth:`build`) are provided
	   to create contexts from configuration files or distribution builders.
	"""

	def __init__(self,
		config: AmpelConfig,
		db: 'AmpelDB',
		loader: 'UnitLoader', # forward reference to avoid cyclic import issues
		resource: dict[str, Any] | None = None,
		admin_msg: str | None = None
	) -> None:
		"""
		Initialize a new context with configuration, database, and unit loader.

		:param config: ampel configuration
		:param db: ampel database object instance
		:param loader: Unit loader for instantiating units
		:param resource: Optional shared resources dictionary
		"""

		self.config = config
		self.db = db
		self.loader = loader
		self.admin_msg = admin_msg
		self.resource = resource
		self.run_time_aliases: dict[str, Any] = {}
		self.uuid = uuid.uuid4()
		
		# try to register aux units globally
		try:
			AuxUnitRegister.initialize(config)
		except Exception:
			print("UnitLoader auxiliary units auto-registration failed")  # noqa: T201


	@classmethod
	def load(cls,
		config: str | dict,
		pwd_file_path: str | None = None,
		pwds: Iterable[str] | None = None,
		freeze_config: bool = True,
		vault: AmpelVault | None = None,
		one_db: bool = False,
		**kwargs
	) -> Self:
		"""
		Instantiate a new context from a configuration file or dict.

		:param config: local path to an ampel config file (yaml or json) or loaded config as dict
		:param pwd_file_path: path to a text file containing one password per line.
		The underlying AmpelVault will be initialized with an AESecretProvider configured with these pwds.
		:param pwds: Same as 'pwd_file_path' except a list of passwords is provided directly via this parameter.
		:param freeze_config:
			whether to convert the elements contained of ampel config
			into immutable structures (:class:`dict` ->
			:class:`~ampel.view.ReadOnlyDict.ReadOnlyDict`, :class:`list` -> :class:`tuple`).
			Parameter does only apply if the config is loaded by this method, i.e if parameter 'config' is a str.
		"""

		# Avoid cyclic import issues
		from ampel.core.AmpelDB import AmpelDB  # noqa: PLC0415
		from ampel.core.UnitLoader import UnitLoader  # noqa: PLC0415

		alconf = AmpelConfig(config) if isinstance(config, dict) else AmpelConfig.load(config)
		if vault is None:
			vault = AmpelVault([])

		if pwd_file_path and not pwds:
			with open(pwd_file_path) as f:
				pwds = [l.strip() for l in f.readlines()]

		if pwds:
			# AESecretProvider is optional
			from ampel.secret.AESecretProvider import AESecretProvider  # noqa: PLC0415
			vault.providers.append(
				AESecretProvider(pwds)
			)

		db = AmpelDB.new(alconf, vault, one_db=one_db)

		return cls(
			config = alconf,
			db = db,
			loader = UnitLoader(config=alconf, db=db, vault=vault),
			**kwargs
		)


	@classmethod
	def build(cls,
		ignore_errors: bool = False,
		pwd_file_path: None | str = None,
		pwds: None | Iterable[str] = None,
		freeze_config: bool = True,
		verbose: bool = False
	) -> Self:
		"""
		Instantiate a new context by building configuration from local distributions.

		Gathers and merges all available configurations from Ampel distributions found
		locally and morphs them into a final configuration. This is a convenience method and
		not intended for production deployments.

		:param ignore_errors: If True, continue building despite non-fatal errors.
		:param pwd_file_path: Path to a text file containing one password per line.
		:param pwds: Passwords provided directly; alternative to ``pwd_file_path``.
		:param freeze_config: Whether to convert the built config into immutable structures.
		:param verbose: If True, show verbose distribution loading output.
		:returns: A new :class:`AmpelContext` instance.
		"""

		if pwd_file_path:
			with open(pwd_file_path) as f:
				pwds = [l.strip() for l in f.readlines()]

		# Import here to avoid cyclic import error
		from ampel.config.builder.DistConfigBuilder import DistConfigBuilder # noqa: PLC0415
		from ampel.config.builder.DisplayOptions import DisplayOptions # noqa: PLC0415
		cb = DistConfigBuilder(options=DisplayOptions(verbose=verbose))
		cb.load_distributions()

		return cls.load(
			cb.build_config(ignore_errors, pwds=pwds),
			pwd_file_path = pwd_file_path,
			pwds = pwds,
			freeze_config = freeze_config,
		)


	def new_run_id(self) -> int:
		"""
		Return an identifier that can be used to associate log entries from a
		single process invocation. This ID is unique and monotonicaly increasing.
		"""
		return self.db \
			.get_collection('counter') \
			.find_one_and_update(
				{'_id': 'current_run_id'},
				{'$inc': {'value': 1}},
				new=True, upsert=True
			) \
			.get('value')


	def get_config(self) -> AmpelConfig:
		"""
		.. note:: Future versions may support multiple configurations.
		"""
		return self.config


	def get_database(self) -> 'AmpelDB':
		"""
		.. note:: Future versions may support multiple databases.
		"""
		return self.db


	def resolve_conf_id(self, conf_id: int) -> dict[str, Any] | None:
		try:
			return self.config.get_conf_by_id(conf_id)
		# confid not found (obsolete or dynamically generated by process)
		except Exception:
			l = list(self.db.col_conf_ids.find({"_id": conf_id}))
			if len(l) == 0:
				return None
			del l[0]['_id']
			return l[0]


	def add_conf_id(self, conf_id: int, unit_config: dict[str, Any]) -> None:
		self.db.add_conf_id(conf_id, unit_config)
		dict.__setitem__(self.config._config["confid"], conf_id, unit_config)  # noqa: SLF001


	def add_run_time_alias(self, key: str, value: Any, overwrite: bool = False) -> None:
		if not isinstance(key, str) or key[0] != '%' != key[1]:
			raise ValueError('Run time aliases must begin with %%')
		if key in self.run_time_aliases and not overwrite:
			raise ValueError(f"Run time alias {key} already defined, set overwrite=True to ignore")
		self.run_time_aliases[key] = value


	def __repr__(self) -> str:
		return "<AmpelContext>"
