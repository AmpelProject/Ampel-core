#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/cli/AbsCoreCommand.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 18.03.2021
# Last Modified Date: 11.11.2021
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import os
import re
from typing import Sequence, Dict, Any, Optional, TypeVar, Type, Iterator, Tuple
from ampel.abstract.AbsSecretProvider import AbsSecretProvider
from ampel.core.AmpelDB import AmpelDB
from ampel.config.AmpelConfig import AmpelConfig
from ampel.secret.AmpelVault import AmpelVault
from ampel.abstract.AbsCLIOperation import AbsCLIOperation
from ampel.core.AmpelContext import AmpelContext
from ampel.core.UnitLoader import UnitLoader
from ampel.log.AmpelLogger import AmpelLogger
from ampel.util.mappings import set_by_path
from ampel.util.freeze import recursive_freeze

custom_conf_patter = re.compile(r"^--[\w-]*(?:\.[\w-]+)*.*$")

T = TypeVar("T", bound = AmpelContext)

class AbsCoreCommand(AbsCLIOperation, abstract=True):

	def load_config(self,
		config_path: str,
		unknown_args: Sequence[str],
		logger: Optional[AmpelLogger] = None,
		freeze: bool = True,
		env_var_prefix: Optional[str] = "AMPEL_CONFIG_",
	) -> AmpelConfig:

		ampel_conf = AmpelConfig.load(config_path, freeze=False)

		if logger is None:
			logger = AmpelLogger.get_logger()

		if env_var_prefix is not None:
			for var, value in os.environ.items():
				if var.startswith(env_var_prefix):
					k = var[len(env_var_prefix):]
					v = _maybe_int(value)
					logger.info(f"Setting config parameter '{k}' from environment")
					set_by_path(ampel_conf._config, k, v)

		for k, v in self.get_custom_args(unknown_args):
			if ampel_conf.get(".".join(k.split(".")[:-1])) is None:
				logger.info(f"Cannot set ampel config parameter '{k}' (parent key does not exist)")
				continue

			logger.info(f"Setting config parameter '{k}' value to: {v}")
			set_by_path(ampel_conf._config, k, v)

		if freeze:
			ampel_conf._config = recursive_freeze(ampel_conf._config)

		return ampel_conf


	def get_custom_args(self, customizations: Sequence[str]) -> Iterator[Tuple[str, Any]]:

		it = iter(customizations)
		for el in it:
			if custom_conf_patter.match(el):
				if "=" in el:
					s = el.split("=")
					k = s[0][2:]
					v = _maybe_int(s[1])
				else:
					k = el[2:]
					try:
						v = _maybe_int(next(it))
					except StopIteration:
						break
				yield k, v


	def get_vault(self, args: Dict[str, Any]) -> Optional[AmpelVault]:
		vault = None
		if args.get('secrets'):
			from ampel.secret.DictSecretProvider import DictSecretProvider
			from ampel.secret.DirSecretProvider import DirSecretProvider
			if os.path.isdir(args['secrets']):
				provider: AbsSecretProvider = DirSecretProvider(args['secrets'])
			else:
				provider = DictSecretProvider.load(args['secrets'])
			vault = AmpelVault([provider])
		return vault


	def get_db(self,
		config: AmpelConfig,
		vault: Optional[AmpelVault] = None,
		require_existing_db: bool = True,
		one_db: bool = False
	) -> AmpelDB:

		try:
			return AmpelDB.new(
				config,
				vault,
				require_exists = require_existing_db,
				one_db = one_db
			)
		except Exception as e:
			if "Databases with prefix" in str(e):
				s = "Databases with prefix " + config.get('mongo.prefix', str, raise_exc=True) + " do not exist"
				raise SystemExit("\n" + "="*len(s) + "\n" + s + "\n" + "="*len(s) + "\n")
			raise e


	def get_context(self,
		args: Dict[str, Any],
		unknown_args: Sequence[str],
		logger: Optional[AmpelLogger] = None,
		freeze_config: bool = True,
		ContextClass: Type[T] = AmpelContext, # type: ignore[assignment]
		require_existing_db: bool = True,
		one_db: bool = False,
		**kwargs
	) -> T:

		if logger is None:
			logger = AmpelLogger.get_logger()

		config = self.load_config(
			args['config'], unknown_args, logger, freeze = freeze_config
		)

		vault = self.get_vault(args)
		db = self.get_db(config, vault, require_existing_db, one_db)
		return ContextClass(
			config = config,
			db = db,
			loader = UnitLoader(config, db=db, vault=vault),
			**kwargs
		)


	def convert_logical_args(self, name: str, args: Dict[str, Any]) -> None:

		for k in (f"with_{name}", f"with_{name}s_and", f"with_{name}s_or"):
			if args.get(k):
				if name not in args:
					args[name] = {}
				args[name]['with'] = args[k]
				break

		for k in (f"without_{name}", f"without_{name}s_and", f"without_{name}s_or"):
			if args.get(k):
				if name not in args:
					args[name] = {}
				args[name]['without'] = args[k]
				break


def _maybe_int(stringy):
	try:
		return int(stringy)
	except Exception:
		return stringy
