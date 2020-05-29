#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/core/AmpelContext.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 18.02.2020
# Last Modified Date: 18.04.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from dataclasses import dataclass
from typing import Dict, Any, Optional, Literal, Iterable, TYPE_CHECKING
from ampel.config.AmpelConfig import AmpelConfig
from ampel.db.AmpelDB import AmpelDB
from ampel.log.AmpelLogger import AmpelLogger

# Avoid cyclic import issues
if TYPE_CHECKING:
	from ampel.core.UnitLoader import UnitLoader # noqa


@dataclass
class AmpelContext:
	"""
	This class is typically instantiated by Ampel controllers
	and provided to "processor unit" instances.
	Note: this class might in the future be capable
	of handling multiple AmpelConfig/AmpelDB instances
	"""

	config: AmpelConfig
	db: AmpelDB
	loader: 'UnitLoader' # forward reference to avoid cyclic import issues
	tier: Optional[Literal[0, 1, 2, 3]] = None
	resource: Optional[Dict[str, Any]] = None

	admin_msg: Optional[str] = None
	extra: Optional[Dict[str, Any]] = None


	@classmethod
	def new(cls,
		config: AmpelConfig,
		tier: Optional[Literal[0, 1, 2, 3]] = None
	) -> 'AmpelContext':

		if not isinstance(config, AmpelConfig):
			raise ValueError("Illegal value provided with parameter 'config'")

		# Avoid cyclic import issues
		from ampel.core.UnitLoader import UnitLoader # noqa

		return cls(
			config = config,
			db = AmpelDB.new(config),
			loader = UnitLoader(config=config, tier=tier),
			tier = tier
		)

	@classmethod
	def load(cls,
		config_file_path: str,
		pwd_file_path: Optional[str] = None,
		pwds: Optional[Iterable[str]] = None,
		freeze_config: bool = True,
		tier: Optional[Literal[0, 1, 2, 3]] = None
	) -> 'AmpelContext':
		"""
		Instantiates a new AmpelContext instance.
		:param config: either a local path to an ampel config file (yaml or json) \
		or directly an AmpelConfig instance.
		:param pwd_file_path: if provided, the encrypted conf entries possibly contained \
		in the ampel config instance will be decrypted using the provided password file. \
		The password file must define one password per line.
		:param freeze_config: whether to convert the elements contained of ampel config \
		into immutable structures (dict -> ReadOnlyDict, list -> tuple). Parameter does \
		only apply if the config is loaded by this method, i.e if parameter 'config' is a str.
		"""

		return cls.new(
			config = AmpelConfig.load(
				config_file_path, pwd_file_path, pwds, freeze_config
			),
			tier = tier
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
		The required underlying ampel configuration (AmpelConfig) is built from scratch,
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
				cb.build_config(ignore_errors, pwds),
				freeze_config
			),
			tier=tier
		)


	def get_config(self) -> AmpelConfig:
		"""
		Note: in the future, AmpelContext might hold references to multiple different config
		"""
		return self.config


	def get_database(self) -> AmpelDB:
		"""
		Note: in the future, this class might hold references to multiple different databases
		"""
		return self.db


	def get_logger(self, profile: str = 'default', **kwargs) -> AmpelLogger:
		return AmpelLogger.get_logger(
			**{**self.config.get(f'logging.AmpelLogger.{profile}', dict, raise_exc=True), **kwargs}
		)


	def get_unique_logger(self, profile: str = 'default', **kwargs) -> AmpelLogger:
		return AmpelLogger.get_unique_logger(
			**{**self.config.get(f'logging.AmpelLogger.{profile}', dict, raise_exc=True), **kwargs}
		)


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
