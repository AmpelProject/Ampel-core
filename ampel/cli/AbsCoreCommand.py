#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/cli/AbsCoreCommand.py
# License:             BSD-3-Clause
# Author:              valery brinnel <firstname.lastname@gmail.com>
# Date:                18.03.2021
# Last Modified Date:  27.08.2022
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

import re, os
from typing import Any, TypeVar, Literal
from collections.abc import Sequence, Iterator
from ampel.config.AmpelConfig import AmpelConfig
from ampel.abstract.AbsCLIOperation import AbsCLIOperation
from ampel.core.AmpelContext import AmpelContext
from ampel.core.UnitLoader import UnitLoader
from ampel.log.AmpelLogger import AmpelLogger
from ampel.util.mappings import set_by_path
from ampel.util.freeze import recursive_freeze
from ampel.util.pretty import out_stack
from ampel.cli.utils import get_vault, get_db, _maybe_int
from ampel.cli.config import get_user_data_config_path

custom_conf_patter = re.compile(r"^--[\w-]*(?:\.[\w-]+)*.*$")

T = TypeVar("T", bound = AmpelContext)

class AbsCoreCommand(AbsCLIOperation, abstract=True):

	def __init__(self):
		self.parsers = {}

	def load_config(self,
		config_path: None | str,
		unknown_args: Sequence[str],
		logger: None | AmpelLogger = None,
		freeze: bool = True,
		env_var_prefix: None | str = "AMPEL_CONFIG_",
	) -> AmpelConfig:

		if not config_path:
			std_conf = get_user_data_config_path()
			if os.path.exists(std_conf):
				ampel_conf = AmpelConfig.load(std_conf, freeze=False)
			else:
				with out_stack():
					raise ValueError("No default ampel config found -> argument -config required\n")
		else:
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
				with out_stack():
					raise ValueError(f"Unknown config parameter '{k}'\n")

			logger.info(f"Setting config parameter '{k}' value to: {v}")
			set_by_path(ampel_conf._config, k, v)

		if freeze:
			ampel_conf._config = recursive_freeze(ampel_conf._config)

		return ampel_conf


	def get_custom_args(self, customizations: Sequence[str]) -> Iterator[tuple[str, Any]]:
		"""
		:raises: ValueError
		"""

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
						v = None
				yield k, v
			else:
				with out_stack():
					raise ValueError(f"Unknown argument: {el}\n")


	def get_context(self,
		args: dict[str, Any],
		unknown_args: Sequence[str],
		logger: None | AmpelLogger = None,
		freeze_config: bool = True,
		ContextClass: type[T] = AmpelContext, # type: ignore[assignment]
		require_existing_db: bool | str = True,
		one_db: bool | Literal['auto'] = False,
		**kwargs
	) -> T:
		"""
		:require_existing_db: str typed values specify required database prefix
		"""

		if logger is None:
			logger = AmpelLogger.get_logger()

		config = self.load_config(
			args['config'], unknown_args, logger, freeze = freeze_config
		)

		vault = get_vault(args)
		db = get_db(config, vault, require_existing_db, one_db)
		return ContextClass(
			config = config,
			db = db,
			loader = UnitLoader(config, db=db, vault=vault),
			**kwargs
		)


	def convert_logical_args(self, name: str, args: dict[str, Any]) -> None:

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
