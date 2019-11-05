#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/config/builder/ConfigCollector.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 16.10.2019
# Last Modified Date: 25.10.2019
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Dict, Any, List, Union
from ampel.abstract.AmpelABC import AmpelABC, abstractmethod
from ampel.logging.AmpelLogger import AmpelLogger


class ConfigCollector(dict, metaclass=AmpelABC):
	"""
	"""

	def __init__(
		self, conf_section: str, content: Dict = None, 
		logger: AmpelLogger = None, verbose: bool = False
	):
		""" """
		super().__init__(content if content else {})
		self.verbose = verbose
		self.has_error = False
		self.conf_section = conf_section
		self.logger = AmpelLogger.get_logger() if logger is None else logger


	@abstractmethod
	def add(self, arg: Union[Dict[str, Any], List[str]], dist_name: str = None) -> None:
		""" """


	def error(self, msg: str, exc_info=None) -> None:
		""" """
		self.logger.error(msg, exc_info=exc_info)
		self.has_error = True


	def missing_key(self, what: str, key: str, dist_name: str) -> None:
		""" """
		self.error(
			f"{what} dict is missing key '{key}' {self.distrib_hint(dist_name)}"
		)


	def duplicated_entry(
		self, conf_key: str, prev_dist: str = None,
		new_dist: str = None, section_detail: str = None
	) -> None:
		""" """

		from string import Template
		t = Template(
			"Duplicated $what definition: '$conf_key'\n" + 
			"Previously set by ampel distrib: $prev\n" + 
			"Redefined by ampel distrib: $new"
		)

		unknown = "unknown distribution (config manually loaded?)"

		self.error(
			t.substitute(
				what = section_detail if section_detail else self.conf_section, 
				conf_key = conf_key, 
				prev = prev_dist if prev_dist else self.get(conf_key, {}).get('distName', unknown),
				new = new_dist if new_dist else unknown
			)
		)


	@staticmethod
	def distrib_hint(distrib: str) -> str:
		""" """
		return f"(ampel distribution: {distrib})" if distrib else ""
