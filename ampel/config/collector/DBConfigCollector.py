#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/config/builder/collector/DBConfigCollector.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 16.10.2019
# Last Modified Date: 25.10.2019
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Dict, Any
from ampel.config.collector.ConfigCollector import ConfigCollector


class DBConfigCollector(ConfigCollector):
	"""
	"""
	def add(self, arg: Dict[str, Any], dist_name: str = None) -> None:
		""" """
		dbs = self.get('databases')
		if not dbs:
			dbs = []
			self.__setitem__('prefix', 'Ampel')
			self.__setitem__('databases', dbs)
		dbs.append(arg)
