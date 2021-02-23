#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/dev/DevAmpelContext.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 10.06.2020
# Last Modified Date: 10.02.2021
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Optional, Any, Dict, Union, List
from ampel.core import AmpelContext, UnitLoader
from ampel.util.freeze import recursive_unfreeze
from ampel.config.AmpelConfig import AmpelConfig
from ampel.db.AmpelDB import AmpelDB
from ampel.model.ChannelModel import ChannelModel
from ampel.util.mappings import set_by_path, build_unsafe_short_dict_id
from ampel.dev.DictSecretProvider import PotemkinSecretProvider


class DevAmpelContext(AmpelContext):


	def __init__(self,
		db_prefix: Optional[str] = None, purge_db: bool = False,
		custom_conf: Optional[Dict[str, Any]] = None, **kwargs
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
			dict.__setitem__(self.config._config['db'], 'prefix', db_prefix)

		if custom_conf or db_prefix:
			conf = self._get_unprotected_conf()
			for k, v in (custom_conf or {}).items():
				set_by_path(conf, k, v)
			self._set_new_conf(conf)

		if not self.loader.secrets:
			self.loader.secrets = PotemkinSecretProvider()

		if purge_db:
			self.db.drop_all_databases()
			self.db.init_db()


	def add_channel(self, name: Union[int, str], access: List[str] = []):
		cm = ChannelModel(channel=name, access=access)
		conf = self._get_unprotected_conf()
		for k, v in cm.dict().items():
			set_by_path(conf, f"channel.{name}.{k}", v)
		self._set_new_conf(conf)


	def add_config_id(self, arg: Dict[str, Any]) -> int:
		conf_id = build_unsafe_short_dict_id(arg)
		if conf_id in self.config.get("confid", dict, raise_exc=True):
			return conf_id
		conf = self._get_unprotected_conf()
		conf["confid"][conf_id] = arg
		self._set_new_conf(conf)
		return conf_id


	def _get_unprotected_conf(self) -> Dict[str, Any]:
		if self.config.is_frozen():
			return recursive_unfreeze(self.config._config) # type: ignore[arg-type]
		return self.config._config


	def _set_new_conf(self, conf: Dict[str, Any]) -> None:
		self.config = AmpelConfig(conf, True)
		self.loader = UnitLoader(self.config, secrets=self.loader.secrets)
		self.db = AmpelDB.new(self.config, self.loader.secrets)
