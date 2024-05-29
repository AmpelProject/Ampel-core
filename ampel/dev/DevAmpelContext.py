#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/dev/DevAmpelContext.py
# License:             BSD-3-Clause
# Author:              valery brinnel <firstname.lastname@gmail.com>
# Date:                10.06.2020
# Last Modified Date:  01.04.2023
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

from collections.abc import Sequence
from typing import Any

from ampel.base.AmpelUnit import AmpelUnit
from ampel.base.AuxUnitRegister import AuxUnitRegister
from ampel.base.LogicalUnit import LogicalUnit
from ampel.config.AmpelConfig import AmpelConfig
from ampel.core.AmpelContext import AmpelContext
from ampel.core.AmpelDB import AmpelDB
from ampel.core.ContextUnit import ContextUnit
from ampel.core.UnitLoader import UnitLoader
from ampel.model.ChannelModel import ChannelModel
from ampel.model.UnitModel import UnitModel
from ampel.util.freeze import recursive_unfreeze
from ampel.util.mappings import set_by_path


class DevAmpelContext(AmpelContext):

	def __init__(self,
		db_prefix: None | str = None, purge_db: bool = False,
		custom_conf: None | dict[str, Any] = None, **kwargs
	) -> None:
		"""
		:db_prefix: customizes the db prefix name
		(ex: "AmpelTest" will result in the databases: AmpelTest_data, AmpelTest_var, AmpelTest_ext)
		:purge_db: wheter all databases with the given prefix should be deleted (purged)
		:extra_conf: convenience parameter which allows to overwrite given parameters of the underlying
		ampel config (possibly frozen) dictionnary. Nested dict keys such as 'general.something' are supported.
		"""

		super().__init__(**kwargs)

		if db_prefix:
			dict.__setitem__(self.config._config['mongo'], 'prefix', db_prefix)  # noqa: SLF001

		if custom_conf or db_prefix:
			conf = self._get_unprotected_conf()
			for k, v in (custom_conf or {}).items():
				set_by_path(conf, k, v)
			self._set_new_conf(conf)

		if purge_db:
			self.db.drop_all_databases()
			self.db.init_db()

		for stored_conf in self.db.get_collection("conf").find({}):
			confid = stored_conf.pop("_id")
			dict.__setitem__(self.config._config["confid"], confid, stored_conf)  # noqa: SLF001


	def add_channel(self, name: int | str, access: Sequence[str] = ()):
		cm = ChannelModel(channel=name, access=access, version=0)
		conf = self._get_unprotected_conf()
		for k, v in cm.__dict__.items():
			set_by_path(conf, f"channel.{name}.{k}", v)
		self._set_new_conf(conf)


	def register_units(self, *Classes: type[AmpelUnit]) -> None:
		for Class in Classes:
			self.register_unit(Class)


	def register_unit(self, Class: type[AmpelUnit]) -> type[AmpelUnit]:

		dict.__setitem__(
			self.config._config['unit'],  # noqa: SLF001
			Class.__name__,
			{
				'fqn': Class.__module__,
				'base': [el.__name__ for el in Class.__mro__[:-1] if 'ampel' in el.__module__],
				'distrib': 'unspecified',
				'file': 'unspecified',
				'version': 'unspecified'
			}
		)

		if issubclass(Class, LogicalUnit | ContextUnit):

			if self.loader._dyn_register is None:  # noqa: SLF001
				self.loader._dyn_register = {}  # noqa: SLF001

			self.loader._dyn_register[Class.__name__] = Class  # noqa: SLF001

		else:
			AuxUnitRegister._dyn[Class.__name__] = Class  # noqa: SLF001

		return Class

	def _get_unprotected_conf(self) -> dict[str, Any]:
		if self.config.is_frozen():
			return recursive_unfreeze(self.config._config) # type: ignore[arg-type]  # noqa: SLF001
		return self.config._config  # noqa: SLF001


	def _set_new_conf(self, conf: dict[str, Any]) -> None:
		self.config = AmpelConfig(conf, True)
		self.db = AmpelDB.new(self.config, self.loader.vault, self.db.require_exists, self.db.one_db)
		self.loader = UnitLoader(self.config, db=self.db, vault=self.loader.vault)


	def new_context_unit(self, unit: str, **kwargs) -> ContextUnit:
		"""
		Convenience method (for notebooks mainly).
		Limitation: config of underlying unit cannot contain/define field 'unit'
		"""
		return self.loader.new_context_unit(
			context = self,
			model = UnitModel(unit = unit, config = kwargs)
		)
