#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/core/AmpelContext.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 18.02.2020
# Last Modified Date: 18.02.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import typing
from dataclasses import dataclass
from typing import Dict, Any, Optional, Literal
from ampel.db.AmpelDB import AmpelDB
from ampel.config.AmpelConfig import AmpelConfig

if typing.TYPE_CHECKING:
	from ampel.core.AmpelUnitLoader import AmpelUnitLoader # noqa


@dataclass
class AmpelContext:
	"""
	This class is typically instantiated by Ampel controllers
	and provided to child AbsProcessorUnit instances.
	Note: this class might in the future be capable
	of handling multiple AmpelConfig/AmpelDB instances
	"""

	config: AmpelConfig
	db: AmpelDB
	loader: 'AmpelUnitLoader'
	tier: Optional[Literal[0, 1, 2, 3]] = None
	admin_msg: Optional[str] = None
	resource: Optional[Dict[str, Any]] = None
	extra: Optional[Dict[str, Any]] = None


	@classmethod
	def from_config(cls, config: AmpelConfig, tier: Literal[0, 1, 2, 3]):
		# Avoid cyclic import issues
		from ampel.core.AmpelUnitLoader import AmpelUnitLoader # noqa
		return cls(
			config = config,
			db = AmpelDB.from_config(config),
			loader = AmpelUnitLoader(config=config, tier=tier)
		)


	def get_config(self) -> AmpelConfig:
		"""
		Note: in the future, AmpelContext might hold references to multiple different config
		"""
		return self.config


	def get_database(self) -> AmpelDB:
		"""
		Note: in the future, AmpelContext might hold references to multiple different config
		"""
		return self.db

	def __repr__(self) -> str:
		return "<AmpelContext>"

#	@root_validator
#	def _set_defaults(cls, values):
#		"""
#		Instantiates AmpelDB with default settings if parameter "db" is not provided
#		Instantiates AmpelUnitLoader if parameter "loader" is not provided
#		"""
#		if not values.get('db'):
#			values['db'] = AmpelDB.from_config(values['config'])
#		if not values.get('loader'):
#			values['loader'] = AmpelUnitLoader(
#				values['config'], tier=values.get('tier')
#			)
#		return values
