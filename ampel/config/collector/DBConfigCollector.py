#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/config/collector/DBConfigCollector.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 16.10.2019
# Last Modified Date: 06.02.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Dict, Any, Optional
from ampel.model.db.AmpelDBModel import AmpelDBModel
from ampel.config.collector.ConfigCollector import ConfigCollector
from ampel.config.collector.AbsDictConfigCollector import AbsDictConfigCollector


class DBConfigCollector(AbsDictConfigCollector):

	def add(self,
		arg: Dict[str, Any],
		file_name: Optional[str] = None,
		dist_name: Optional[str] = None
	) -> None:
		""" """
		dbs = self.get('databases')

		if not dbs:
			dbs = []
			self.__setitem__('prefix', 'Ampel')
			self.__setitem__('databases', dbs)

		# validate model
		if self.verbose:
			self.logger.verbose("Validating DB configuration")

		try:

			m = AmpelDBModel(**arg)

			if self.verbose:
				self.logger.verbose(f"Configuration of DB collection '{m.name}' is valid")

			dbs.append(arg)

		except Exception:
			self.error(
				"Incorrect DB configuration " +
				ConfigCollector.distrib_hint(file_name, dist_name)
			)
