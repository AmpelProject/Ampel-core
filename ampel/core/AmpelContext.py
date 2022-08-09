#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/core/AmpelContext.py
# License:             BSD-3-Clause
# Author:              valery brinnel <firstname.lastname@gmail.com>
# Date:                18.02.2020
# Last Modified Date:  09.01.2022
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

from typing import Any, Literal, TYPE_CHECKING
from collections.abc import Iterable
from ampel.config.AmpelConfig import AmpelConfig
from ampel.base.AuxUnitRegister import AuxUnitRegister
from ampel.secret.AmpelVault import AmpelVault
from ampel.secret.AESecretProvider import AESecretProvider
from ampel.config.builder.DisplayOptions import DisplayOptions

# Avoid cyclic import issues
if TYPE_CHECKING:
	from ampel.core.AmpelDB import AmpelDB
	from ampel.core.UnitLoader import UnitLoader # noqa


class AmpelContext:
	"""
	This class is typically instantiated by Ampel controllers
	and provided to "processor unit" instances.
	Note: this class might in the future be capable
	of handling multiple AmpelConfig/AmpelDB instances
	"""

	def __init__(self,
		config: AmpelConfig,
		db: 'AmpelDB',
		loader: 'UnitLoader', # forward reference to avoid cyclic import issues
		resource: None | dict[str, Any] = None,
		admin_msg: None | str = None
	) -> None:
		"""
		:param config: system configuration
		:param db: database client
		:param loader: instantiates unit from :class:`~ampel.model.UnitModel.UnitModel`
		"""

		self.config = config
		self.db = db
		self.loader = loader
		self.admin_msg = admin_msg
		self.resource = resource
		
		# try to register aux units globally
		try:
			AuxUnitRegister.initialize(config)
		except Exception:
			print("UnitLoader auxiliary units auto-registration failed")


	@classmethod
	def load(cls,
		config: str | dict,
		pwd_file_path: None | str = None,
		pwds: None | Iterable[str] = None,
		freeze_config: bool = True,
		vault: None | AmpelVault = None,
		one_db: bool = False,
		**kwargs
	) -> 'AmpelContext':
		"""
		Instantiates a new AmpelContext instance.

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
		from ampel.core.UnitLoader import UnitLoader # noqa
		from ampel.core.AmpelDB import AmpelDB

		alconf = AmpelConfig(config) if isinstance(config, dict) else AmpelConfig.load(config)
		if vault is None:
			vault = AmpelVault([])

		if pwds:
			vault.providers.append(
				AESecretProvider(pwds)
			)
		elif pwd_file_path:
			with open(pwd_file_path, "r") as f:
				vault.providers.append(
					AESecretProvider([l.strip() for l in f.readlines()])
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
		verbose: bool = False,
	) -> 'AmpelContext':
		"""
		Instantiates a new AmpelContext instance.

		The required underlying ampel configuration (:class:`~ampel.config.AmpelConfig.AmpelConfig`) is built from scratch,
		meaning all available configurations defined in the various ampel repositories available locally
		are collected, merged merged together and morphed the info into the final ampel config.
		This is a convenience method, do not use for production
		"""

		if pwd_file_path:
			with open(pwd_file_path, "r") as f:
				pwds = [l.strip() for l in f.readlines()]

		# Import here to avoid cyclic import error
		from ampel.config.builder.DistConfigBuilder import DistConfigBuilder
		cb = DistConfigBuilder(options=DisplayOptions(verbose=verbose))
		cb.load_distributions()

		return cls.load(
			cb.build_config(ignore_errors, pwds=pwds),
			pwd_file_path=pwd_file_path,
			pwds=pwds,
			freeze_config=freeze_config,
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
		.. note:: in the future, AmpelContext might hold references to multiple different config
		"""
		return self.config


	def get_database(self) -> 'AmpelDB':
		"""
		.. note:: in the future, this class might hold references to multiple different databases
		"""
		return self.db


	def resolve_conf_id(self, conf_id: int) -> None | dict[str, Any]:
		try:
			return self.config.get_conf_id(conf_id)
		# confid not found (obsolete or dynamically generated by process)
		except Exception:
			l = list(self.db.col_conf_ids.find({"_id": conf_id}))
			if len(l) == 0:
				return None
			del l[0]['_id']
			return l[0]


	def __repr__(self) -> str:
		return "<AmpelContext>"


	def deactivate_processes(self) -> None:
		""" """
		for i in range(4):
			for p in self.config.get(f'process.t{i}', dict, raise_exc=True).values():
				p['active'] = False


	def activate_process(self,
		name: str,
		tier: None | Literal[0, 1, 2, 3] = None
	) -> bool:
		"""
		:returns: False if process was deactivated, True if not found
		"""
		for i in range(4):
			if tier and i != tier:
				continue
			if p := self.config.get(f'process.t{i}.{name}', dict):
				p['active'] = False
				return False
		return True
