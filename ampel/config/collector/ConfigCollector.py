#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/config/collector/AbsConfigCollector.py
# License:             BSD-3-Clause
# Author:              valery brinnel <firstname.lastname@gmail.com>
# Date:                16.10.2019
# Last Modified Date:  03.03.2020
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

from typing import Any, Literal
from ampel.log.AmpelLogger import AmpelLogger


class ConfigCollector(dict):

	def __init__(self,
		conf_section: str,
		content: None | dict = None,
		logger: None | AmpelLogger = None,
		verbose: bool = False,
		debug: bool = False,
		tier: None | Literal[0, 1, 2, 3, "ops"] = None
	) -> None:

		super().__init__(**content if content else {})
		self.verbose = verbose
		self.has_error = False
		self.conf_section = conf_section
		self.logger = AmpelLogger.get_logger() if logger is None else logger
		self.tier = f'T{tier}' if tier is not None else 'general'

		if debug:
			self.logger.info(
				f'Creating {self.__class__.__name__} collector '
				f'for T{self.tier} config section {conf_section}'
			)


	def error(self, msg: str, exc_info: None | Any = None) -> None:
		self.logger.error(msg, exc_info=exc_info)
		self.has_error = True


	def missing_key(self,
		what: str, key: str, dist_name: str, register_file: None | str
	) -> None:
		self.error(
			f'{what} dict is missing key "{key}" '
			f'{self.distrib_hint(dist_name, register_file)}'
		)


	def duplicated_entry(self,
		conf_key: str,
		prev_file: None | str = None,
		prev_dist: None | str = None,
		new_file: None | str = None,
		new_dist: None | str = None,
		section_detail: None | str = None
	) -> None:

		from string import Template
		t = Template(
			'Duplicated $what definition: "$conf_key"\n' +
			'Previously set by $prev\n' +
			'Redefined by $new'
		)

		if prev_file is None:
			prev_file = self.get(conf_key, {}).get('source', 'unknown')

		if prev_dist is None:
			prev_dist = self.get(conf_key, {}).get('distrib', 'unknown')

		prev = self.distrib_hint(prev_dist, prev_file, False)
		new = self.distrib_hint(new_dist, new_file, False)

		self.error(
			t.substitute(
				what = section_detail if section_detail else self.conf_section,
				conf_key = conf_key,
				prev = prev,
				new = new if new else 'unknown',
			)
		)


	@staticmethod
	def distrib_hint(
		distrib: None | str = None,
		file_register: None | str = None,
		parenthesis: bool = True
	) -> str:
		""" Adds distribution name if available """

		ret = ''

		if file_register:
			if parenthesis:
				ret += '('
			ret += f'conf file: {file_register}'

		if distrib:
			if ret:
				return f'{ret} from distribution: {distrib}{")" if parenthesis else ""}'
			return f'(distribution: {distrib})' if parenthesis else f'distribution: {distrib}'

		if ret:
			return f'{ret})' if parenthesis else ret

		return ret
