#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/config/builder/BaseConfig.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 16.10.2019
# Last Modified Date: 29.01.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import json
from typing import Optional, Dict, Any
from ampel.config.collector.UnitConfigCollector import UnitConfigCollector
from ampel.config.collector.AliasConfigCollector import AliasConfigCollector
from ampel.config.collector.ProcessConfigCollector import ProcessConfigCollector
from ampel.config.collector.DBConfigCollector import DBConfigCollector
from ampel.config.collector.ChannelConfigCollector import ChannelConfigCollector
from ampel.config.collector.ResouceConfigCollector import ResouceConfigCollector

from ampel.logging.AmpelLogger import AmpelLogger

class BaseConfig(dict):
	"""
	Class used to aggregate config chunks into a central configuration dict for ampel.
	"""

	general_keys = {
		"channel": ChannelConfigCollector,
		"db": DBConfigCollector,
		"resource": ResouceConfigCollector
	}

	tier_keys = {
		"controller": UnitConfigCollector,
		"processor": UnitConfigCollector,
		"unit": UnitConfigCollector,
		"alias": AliasConfigCollector,
		"process": ProcessConfigCollector
	}


	def __init__(self, logger: AmpelLogger = None, verbose: bool = False) -> None:
		""" """
		self.logger = AmpelLogger.get_logger() if logger is None else logger

		d: Dict[str, Any] = {
			**{
				k: Klass(conf_section=k, logger=logger, verbose=verbose)
				for k, Klass in self.general_keys.items()
			},
			'pwd': [],
			**{
				f"t{k}": {
					kk: Klass(tier=k, conf_section=kk, logger=logger, verbose=verbose)
					for kk, Klass in self.tier_keys.items()
				}
				for k in (0, 1, 2, 3)
			}
		}

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
						self.logger.info(f"T{v.tier} {v.__class__.__name__} has errors") # type: ignore
					else:
						self.logger.info(f"{v} has errors")
				if self.has_nested_error(v):
					ret = True

		return ret


	def print(self) -> None:
		""" """
		if self.has_nested_error(): 
			self.logger.info(
				"Warning: error were reported while collecting configurations"
			)

		self.logger.info(
			json.dumps(self, indent=4)
		)
