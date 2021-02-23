#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/core/AmpelContext.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 18.02.2020
# Last Modified Date: 17.02.2021
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from dataclasses import dataclass
from typing import Dict, Any, Optional, Literal, Iterable, TYPE_CHECKING
from ampel.config.AmpelConfig import AmpelConfig
from ampel.base.AuxUnitRegister import AuxUnitRegister

# Avoid cyclic import issues
if TYPE_CHECKING:
	from ampel.db.AmpelDB import AmpelDB
	from ampel.core.UnitLoader import UnitLoader # noqa


@dataclass
class AmpelContext:
	"""
	This class is typically instantiated by Ampel controllers
	and provided to "processor unit" instances.
	Note: this class might in the future be capable
	of handling multiple AmpelConfig/AmpelDB instances
	"""

	#: System configuration
	config: AmpelConfig
	#: Database client
	db: 'AmpelDB'
	#: Instantiates a unit from a :class:`~ampel.model.UnitModel.UnitModel`.
	loader: 'UnitLoader' # forward reference to avoid cyclic import issues
	#: Context is valid for this processing tier only (if None, valid for all).
	tier: Optional[Literal[0, 1, 2, 3]] = None
	resource: Optional[Dict[str, Any]] = None

	admin_msg: Optional[str] = None
	extra: Optional[Dict[str, Any]] = None


	@classmethod
	def new(cls,
		config: AmpelConfig,
		tier: Optional[Literal[0, 1, 2, 3]] = None,
		**kwargs
	) -> 'AmpelContext':

		if not isinstance(config, AmpelConfig):
			raise ValueError("Illegal value provided with parameter 'config'")

		# Avoid cyclic import issues
		from ampel.core.UnitLoader import UnitLoader # noqa
		from ampel.db.AmpelDB import AmpelDB

		secrets = kwargs.pop("secrets", None)

		# try to register aux units globally
		try:
			AuxUnitRegister.initialize(
				config.get("unit.aux", ret_type=dict, raise_exc=True)
			)
		except Exception:
			print("UnitLoader auxiliary units auto-registration failed")

		return cls(
			config = config,
			db = AmpelDB.new(config, secrets),
			loader = UnitLoader(config=config, tier=tier, secrets=secrets),
			tier = tier,
			**kwargs
		)


	@classmethod
	def load(cls,
		config_file_path: str,
		pwd_file_path: Optional[str] = None,
		pwds: Optional[Iterable[str]] = None,
		freeze_config: bool = True,
		tier: Optional[Literal[0, 1, 2, 3]] = None,
		**kwargs
	) -> 'AmpelContext':
		"""
		Instantiates a new AmpelContext instance.

		:param config:
			either a local path to an ampel config file (yaml or json)
			or directly an AmpelConfig instance.
		:param pwd_file_path:
			if provided, the encrypted conf entries possibly contained in the
			ampel config instance will be decrypted using the provided password file.
			The password file must define one password per line.
		:param freeze_config:
			whether to convert the elements contained of ampel config
			into immutable structures (:class:`dict` ->
			:class:`~ampel.view.ReadOnlyDict.ReadOnlyDict`, :class:`list` -> :class:`tuple`).
			Parameter does only apply if the config is loaded by this method, i.e if parameter 'config' is a str.
		"""

		return cls.new(
			config = AmpelConfig.load(config_file_path, pwd_file_path, pwds, freeze_config),
			tier = tier, **kwargs
		)


	@classmethod
	def build(cls,
		ignore_errors: bool = False,
		pwd_file_path: Optional[str] = None,
		pwds: Optional[Iterable[str]] = None,
		freeze_config: bool = True,
		tier: Optional[Literal[0, 1, 2, 3]] = None,
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
		cb = DistConfigBuilder(verbose=verbose)
		cb.load_distributions()

		return cls.new(
			AmpelConfig(
				cb.build_config(ignore_errors, pwds=pwds),
				freeze_config
			),
			tier=tier
		)


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


	def __repr__(self) -> str:
		return "<AmpelContext>"


	def deactivate_processes(self) -> None:
		""" """
		for i in range(4):
			for p in self.config.get(f'process.t{i}', dict, raise_exc=True).values():
				p['active'] = False


	def activate_process(self,
		name: str,
		tier: Optional[Literal[0, 1, 2, 3]] = None
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
