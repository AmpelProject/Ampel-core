#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/dev/DevAmpelContext.py
# License:             BSD-3-Clause
# Author:              valery brinnel <firstname.lastname@gmail.com>
# Date:                10.06.2020
# Last Modified Date:  09.01.2022
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

from typing import Any
from importlib import import_module
from ampel.base.AuxUnitRegister import AuxUnitRegister
from ampel.base.LogicalUnit import LogicalUnit
from ampel.core.ContextUnit import ContextUnit
from ampel.core.AmpelDB import AmpelDB
from ampel.core.UnitLoader import UnitLoader
from ampel.core.AmpelContext import AmpelContext
from ampel.config.AmpelConfig import AmpelConfig
from ampel.model.UnitModel import UnitModel
from ampel.model.ChannelModel import ChannelModel
from ampel.log.AmpelLogger import AmpelLogger
from ampel.util.freeze import recursive_unfreeze
from ampel.util.hash import build_unsafe_dict_id
from ampel.util.mappings import set_by_path
from ampel.util.recursion import walk_and_process_dict


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
			dict.__setitem__(self.config._config['mongo'], 'prefix', db_prefix)

		if custom_conf or db_prefix:
			conf = self._get_unprotected_conf()
			for k, v in (custom_conf or {}).items():
				set_by_path(conf, k, v)
			self._set_new_conf(conf)

		if purge_db:
			self.db.drop_all_databases()
			self.db.init_db()


	def add_channel(self, name: int | str, access: list[str] = []):
		cm = ChannelModel(channel=name, access=access, version=0)
		conf = self._get_unprotected_conf()
		for k, v in cm.__dict__.items():
			set_by_path(conf, f"channel.{name}.{k}", v)
		self._set_new_conf(conf)


	def register_units(self, *Classes: type[LogicalUnit]) -> None:
		for Class in Classes:
			self.register_unit(Class)


	def register_unit(self, Class: type[LogicalUnit]) -> None:

		dict.__setitem__(
			self.config._config['unit'],
			Class.__name__,
			{
				'fqn': Class.__module__,
				'base': [el.__name__ for el in Class.__mro__[:-1] if 'ampel' in el.__module__],
				'distrib': 'unspecified',
				'file': 'unspecified',
				'version': 'unspecified'
			}
		)

		if issubclass(Class, (LogicalUnit, ContextUnit)):

			if self.loader._dyn_register is None:
				self.loader._dyn_register = {}

			self.loader._dyn_register[Class.__name__] = Class

		else:
			AuxUnitRegister._dyn[Class.__name__] = Class


	def gen_config_id(self, unit: str, arg: dict[str, Any], logger: None | AmpelLogger = None) -> int:

		if logger is None:
			logger = AmpelLogger.get_logger()

		if self.loader._dyn_register and unit in self.loader._dyn_register:
			Unit = self.loader._dyn_register[unit]
			arg = Unit(**arg, logger=logger)._get_trace_content()
		elif fqn := self.config._config['unit'][unit].get('fqn'):
			Unit = getattr(import_module(fqn), fqn.split('.')[-1])
			arg = Unit(**arg, logger=logger)._get_trace_content()
		else:
			logger.warn(
				f"Unit {unit} not installed locally. Building *unsafe* conf dict hash: "
				f"changes in unit defaults between releases will go undetected"
			)

		conf_id = build_unsafe_dict_id(arg)

		if conf_id not in self.config._config["confid"]:
			# Works with ReadOnlyDict
			dict.__setitem__(self.config._config["confid"], conf_id, arg)
			(logger or AmpelLogger.get_logger()).info(f"New conf id generated: {conf_id} for {arg}")

		if conf_id not in self.db.conf_ids:
			self.db.add_conf_id(conf_id, arg)

		return conf_id


	def _get_unprotected_conf(self) -> dict[str, Any]:
		if self.config.is_frozen():
			return recursive_unfreeze(self.config._config) # type: ignore[arg-type]
		return self.config._config


	def _set_new_conf(self, conf: dict[str, Any]) -> None:
		self.config = AmpelConfig(conf, True)
		self.db = AmpelDB.new(self.config, self.loader.vault, self.db.require_exists, self.db.one_db)
		self.loader = UnitLoader(self.config, db=self.db, vault=self.loader.vault)


	def hash_ingest_directive(self, config: dict[str, Any], logger: AmpelLogger) -> dict[str, Any]:

		# Example of conf_dicts keys
		# processor.config.directives.0.combine.0.state_t2.0
 		# processor.config.directives.0.combine.0.state_t2.0.config.t2_dependency.0
 		# processor.config.directives.0.point_t2.0
		conf_dicts: dict[str, dict[str, Any]] = {}

		walk_and_process_dict(
			arg = config,
			callback = self._gather_t2_config_callback,
			match = ['point_t2', 'stock_t2', 'state_t2', 't2_dependency'],
			conf_dicts = conf_dicts
		)

		# This does the trick of processing nested config first
		sorted_conf_dicts = {
			k: conf_dicts[k]
			for k in sorted(conf_dicts, key=len, reverse=True)
		}

		for k, d in sorted_conf_dicts.items():

			t2_unit = d["unit"]
			conf = d.get("config", {})

			if not isinstance(conf, dict) or not conf:
				continue

			if logger.verbose > 1:
				logger.debug("Hashing ingest config elements")

			if override := d.get("override"):
				conf = {**conf, **override}

			if logger.verbose > 1:
				logger.debug("Computing hash")

			confid = self.gen_config_id(t2_unit, conf, logger)
			d['config'] = confid

		return config


	def _gather_t2_config_callback(self, path, k, d, **kwargs) -> None:
		""" Used by hash_ingest_config """
		if d[k]:
			for i, el in enumerate(d[k]):
				kwargs['conf_dicts'][f"{path}.{k}.{i}"] = el


	def new_context_unit(self, unit: str, **kwargs) -> ContextUnit:
		"""
		Convenience method (for notebooks mainly).
		Limitation: config of underlying unit cannot contain/define field 'unit'
		"""
		return self.loader.new_context_unit(
			context = self,
			model = UnitModel(unit = unit, config = kwargs)
		)
