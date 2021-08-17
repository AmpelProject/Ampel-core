#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/cli/AbsCoreCommand.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 18.03.2021
# Last Modified Date: 15.07.2021
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import re
from typing import Sequence, Dict, Any, Optional, TypeVar, Type
from ampel.core.AmpelDB import AmpelDB
from ampel.config.AmpelConfig import AmpelConfig
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
		customizations: Sequence[str],
		logger: Optional[AmpelLogger] = None,
		freeze: bool = True
	) -> AmpelConfig:

		ampel_conf = AmpelConfig.load(config_path, freeze=False)

		if logger is None:
			logger = AmpelLogger.get_logger()

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
						return ampel_conf # TODO: say something ?

				if ampel_conf.get(".".join(k.split(".")[:-1])) is None:
					logger.info(f"Cannot set ampel config parameter '{k}' (parent key does not exist)")
					continue

				logger.info(f"Setting config parameter '{k}' value to: {v}")
				set_by_path(ampel_conf._config, k, v)

		if freeze:
			ampel_conf._config = recursive_freeze(ampel_conf._config)

		return ampel_conf


	def get_context(self,
		args: Dict[str, Any],
		unknown_args: Sequence[str],
		logger: Optional[AmpelLogger] = None,
		freeze_config: bool = True,
		ContextClass: Type[T] = AmpelContext, # type: ignore[assignment]
		**kwargs
	) -> T:

		config = self.load_config(
			args['config'], unknown_args,
			logger or AmpelLogger.get_logger(),
			freeze = freeze_config
		)

		vault = None
		if args.get('vault'):
			from ampel.secret.AmpelVault import AmpelVault
			from ampel.secret.DictSecretProvider import DictSecretProvider
			vault = AmpelVault([DictSecretProvider.load(args['secrets'])])

		db = AmpelDB.new(config, vault)
		return ContextClass(
			config = config,
			db = db,
			loader = UnitLoader(config, db=db, vault=vault),
			**kwargs
		)


def _maybe_int(stringy):
	try:
		return int(stringy)
	except Exception:
		return stringy
