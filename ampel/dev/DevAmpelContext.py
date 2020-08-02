#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/dev/DevAmpelContext.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 10.06.2020
# Last Modified Date: 02.08.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Optional, Any
from ampel.core import AmpelContext, UnitLoader
from ampel.util.freeze import recursive_unfreeze
from ampel.config.AmpelConfig import AmpelConfig
from ampel.db.AmpelDB import AmpelDB
from ampel.util.mappings import set_by_path


class DevAmpelContext(AmpelContext):


	def __init__(self, db_prefix: Optional[str] = None, purge_db: bool = False, **kwargs) -> None:

		super().__init__(**kwargs)

		if db_prefix:
			dict.__setitem__(self.config._config['db'], 'prefix', db_prefix)
			self.db = AmpelDB.new(self.config)

		if purge_db:
			AmpelDB.delete_ampel_databases(self.config, db_prefix or self.config._config['db']['prefix'])


	def set_conf_key(self, key: str, val: Any) -> None:
		"""
		Convenience method which allows to modifiy the underlying ampel config (possibly frozen) dictionnary
		:param key: dict key (nested value such as 'general.something' are supported)
		"""
		conf = recursive_unfreeze(self.config._config) if self.config.is_frozen() else self.config._config # type: ignore
		set_by_path(conf, key, val)
		self.config = AmpelConfig(conf, True)
		self.loader = UnitLoader(self.config)
