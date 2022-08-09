#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/config/collector/DBConfigCollector.py
# License:             BSD-3-Clause
# Author:              valery brinnel <firstname.lastname@gmail.com>
# Date:                16.10.2019
# Last Modified Date:  20.04.2022
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

from typing import Any
from ampel.mongo.model.AmpelDBModel import AmpelDBModel
from ampel.config.collector.ConfigCollector import ConfigCollector
from ampel.config.collector.AbsDictConfigCollector import AbsDictConfigCollector
from ampel.log import VERBOSE


class DBConfigCollector(AbsDictConfigCollector):

	def add(self,
		arg: dict[str, Any],
		dist_name: str,
		version: str | float | int,
		register_file: str
	) -> None:

		# Allow 'mongo': {'prefix': ...} in general ampel.conf
		if len(arg) == 1 and 'prefix' in arg:
			self.__setitem__('prefix', arg['prefix'])
			return

		# Allow 'mongo': {'ingest': ...} in general ampel.conf
		if len(arg) == 1 and 'ingest' in arg:
			self.__setitem__('ingest', arg['ingest'])
			return

		# At this point, arg usually contains content of files
		# contained in pyampel-core/conf/ampel-core/mongo/*

		dbs = self.get('databases')

		if not dbs:
			dbs = []
			self.__setitem__('databases', dbs)
			if 'prefix' not in self:
				self.__setitem__('prefix', 'Ampel')


		# validate model
		if self.verbose:
			self.logger.log(VERBOSE, 'Validating DB configuration')

		try:

			m = AmpelDBModel(**arg)

			if self.verbose:
				self.logger.log(VERBOSE, f'Configuration of DB collection "{m.name}" is valid')

			if m.name not in [db['name'] for db in dbs]:
				dbs.append(arg)
			else:
				for db in dbs:
					if db['name'] == m.name:
						if db['role'] != m.role:
							raise ValueError(
								f"Incompatible database configurations for {m.name} (role mismatch)"
							)
						common_cols = {col['name'] for col in db['collections']} & {col.name for col in m.collections}
						if common_cols:
							raise ValueError(
								f"Incompatible database configurations for {m.name} (collection already defined)"
							)
						for col_def in arg['collections']:
							db['collections'].append(col_def)
						break

		except Exception as e:

			self.error(
				'Incorrect DB configuration ' +
				ConfigCollector.distrib_hint(dist_name, register_file),
				exc_info = e
			)
