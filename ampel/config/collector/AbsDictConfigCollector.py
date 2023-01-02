#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/config/collector/AbsDictConfigCollector.py
# License:             BSD-3-Clause
# Author:              valery brinnel <firstname.lastname@gmail.com>
# Date:                03.03.2020
# Last Modified Date:  03.03.2020
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

from typing import Any
from ampel.base.AmpelABC import AmpelABC
from ampel.base.decorator import abstractmethod
from ampel.config.collector.ConfigCollector import ConfigCollector


class AbsDictConfigCollector(ConfigCollector, AmpelABC, abstract=True):

	def __init__(self, **kwargs) -> None:
		super().__init__(**kwargs)
		# value: tuple[dist_name, version, register_file]
		self._origin: dict[str, tuple[str, None | str | float | int, str]] = {}


	def check_duplicates(self,
		key: str,
		dist_name: str,
		version: str | float | int,
		register_file: str,
		section_detail: None | str = None
	) -> bool:
		if self._origin.get(key):
			self.report_duplicated_entry(
				conf_key = key,
				section_detail = section_detail or f'{self.tier} {self.conf_section}',
				new_dist = dist_name,
				new_file = register_file,
				prev_dist = self._origin[key][0],
				prev_file = self._origin[key][2]
			)
			return True

		self._origin[key] = (dist_name, version, register_file)
		return False


	@abstractmethod
	def add(self,
		arg: dict[str, Any],
		dist_name: str,
		version: str | float | int,
		register_file: str
	) -> None | int:
		...
