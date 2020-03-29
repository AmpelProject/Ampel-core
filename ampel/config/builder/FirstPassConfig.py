#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/config/builder/FirstPassConfig.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 16.10.2019
# Last Modified Date: 20.03.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import json
from typing import Optional, Dict, Any, Callable, Type, Union
from ampel.logging.AmpelLogger import AmpelLogger
from ampel.utils.mappings import flatten_dict, unflatten_dict

from ampel.config.collector.ConfigCollector import ConfigCollector
from ampel.config.collector.LoggingCollector import LoggingCollector
from ampel.config.collector.DBConfigCollector import DBConfigCollector
from ampel.config.collector.UnitConfigCollector import UnitConfigCollector
from ampel.config.collector.AliasConfigCollector import AliasConfigCollector
from ampel.config.collector.ProcessConfigCollector import ProcessConfigCollector
from ampel.config.collector.ChannelConfigCollector import ChannelConfigCollector
from ampel.config.collector.ResouceConfigCollector import ResouceConfigCollector
from ampel.config.collector.ForwardUnitConfigCollector import ForwardUnitConfigCollector
from ampel.config.collector.ForwardProcessConfigCollector import ForwardProcessConfigCollector

ConfigDict = Dict[str, Type[ConfigCollector]]

class FirstPassConfig(dict):
	"""
	Class used to aggregate config pieces into a central configuration dict for ampel.
	"""

	general_keys: ConfigDict = {
		"db": DBConfigCollector,
		"logging": LoggingCollector,
		"resource": ResouceConfigCollector,
		"aux": UnitConfigCollector,
		"channel": ChannelConfigCollector
	}

	tier_keys: Dict[str, Union[Type[ConfigCollector], ConfigDict]] = {
		"unit": {
			"controller": UnitConfigCollector,
			"processor": UnitConfigCollector,
			"aux": UnitConfigCollector,
			"base": UnitConfigCollector
		},
		"alias": AliasConfigCollector,
		"process": ProcessConfigCollector
	}


	def __init__(self, logger: AmpelLogger = None, verbose: bool = False) -> None:
		""" """
		self.logger = AmpelLogger.get_logger() if logger is None else logger
		self.log: Callable = self.logger.verbose if verbose else self.logger.info # type: ignore

		d: Dict[str, Any] = {
			**{
				k: Klass(conf_section=k, logger=logger, verbose=verbose)
				for k, Klass in self.general_keys.items()
			},
			'pwd': [],
			**{
				f"t{k}": unflatten_dict(
					{
						kk: Klass(
							conf_section=kk.split(".")[-1],
							logger=logger, verbose=verbose, tier=k
						)
						for kk, Klass in flatten_dict(self.tier_keys).items()
					}
				)
				for k in (0, 1, 2, 3)
			}
		}

		d['unit'] = ForwardUnitConfigCollector(
			root_config=self, conf_section="unit", # type: ignore
			target_collector_type=UnitConfigCollector,
			logger=logger, verbose=verbose,
		)

		d['process'] = ForwardProcessConfigCollector(
			root_config=self, conf_section="process", # type: ignore
			target_collector_type=ProcessConfigCollector,
			logger=logger, verbose=verbose
		)

		super().__init__(d)


	def unset_errors(self, d: Optional[Dict] = None) -> None:
		""" """
		for v in d.values() if d is not None else self.values():
			if isinstance(v, dict):
				if getattr(v, 'has_error', False):
					v.has_error = False # type: ignore
				self.unset_errors(v)


	def has_nested_error(self, d=None) -> bool:
		""" """
		ret = False

		for v in d.values() if d is not None else self.values():
			if isinstance(v, dict):
				if getattr(v, 'has_error', False):
					ret = True
					if hasattr(v, 'tier'):
						self.log(
							f"{v.tier} {v.__class__.__name__} has errors" # type: ignore
						)
					else:
						self.log(f"{v} has errors")
				if self.has_nested_error(v):
					ret = True

		return ret


	def print(self) -> None:
		""" """
		if self.has_nested_error():
			self.log(
				"Warning: error were reported while collecting configurations"
			)

		self.log(json.dumps(self, indent=4))
