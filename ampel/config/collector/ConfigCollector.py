#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File:                Ampel-core/ampel/config/collector/AbsConfigCollector.py
# License:             BSD-3-Clause
# Author:              valery brinnel <firstname.lastname@gmail.com>
# Date:                16.10.2019
# Last Modified Date:  03.03.2020
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

from string import Template
from typing import Any, Literal

from ampel.config.builder.DisplayOptions import DisplayOptions
from ampel.log.AmpelLogger import AmpelLogger


class ConfigCollector(dict):

	def __init__(self,
		conf_section: str,
		options: DisplayOptions | None = None,
		content: dict | None = None,
		logger: AmpelLogger | None = None,
		tier: Literal[0, 1, 2, 3, "ops"] | None = None,
		ignore_exc: list[str] | None = None
	) -> None:

		super().__init__(**content if content else {})
		self.options = options or DisplayOptions()
		self.verbose = self.options.verbose
		self.has_error = False
		self.conf_section = conf_section
		self.logger = AmpelLogger.get_logger() if logger is None else logger
		self.tier = f'T{tier}' if tier is not None else 'general'
		self.ignore_exc = ignore_exc

		if self.options.debug:
			self.logger.info(
				f'Creating {self.__class__.__name__} '
				f'for {self.tier} config section "{conf_section}"'
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


	def report_duplicated_entry(self,
		conf_key: str,
		prev_file: str,
		prev_dist: str,
		new_file: str,
		new_dist: str,
		section_detail: None | str = None
	) -> None:

		t = Template(
			'Duplicated $what definition: "$conf_key"\n' +
			' Previously defined by $prev\n' +
			' Redefined by $new'
		)

		prev = self.distrib_hint(prev_dist, prev_file)
		new = self.distrib_hint(new_dist, new_file)

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
		file_register: None | str = None
	) -> str:
		""" Adds distribution name if available """

		ret = ''

		if file_register:
			ret += file_register

		if distrib:
			if ret:
				return f'{ret} ({distrib})'
			return f'({distrib})'

		if ret:
			return f'{ret})'

		return ret
