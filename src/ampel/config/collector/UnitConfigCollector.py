#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/config/builder/UnitConfigCollector.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 16.10.2019
# Last Modified Date: 24.10.2019
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import re
from typing import List
from ampel.common.AmpelUtils import AmpelUtils
from ampel.config.collector.TierConfigCollector import TierConfigCollector


class UnitConfigCollector(TierConfigCollector):
	"""
	"""

	# pylint: disable=inconsistent-return-statements
	def add(self, arg: List[str], dist_name: str = None) -> None:
		"""
		"""
		for el in AmpelUtils.iter(arg):

			try:

				# pylint: disable=anomalous-backslash-in-string
				k = re.sub(".*\.", "", el)

				if self.get(k):
					self.duplicated_entry(
						conf_key=k, section_detail=f"T{self.tier} {self.conf_section}",
						new_dist=dist_name
					)
					continue

				self.__setitem__(k, {'fqn': el, 'distName': dist_name})

				if self.verbose:
					self.logger.verbose(f"-> Adding T{self.tier} {self.conf_section}: {k}")

			except Exception as e:
				self.error(
					f"Error occured while loading {self.conf_section} " +
					self.distrib_hint(dist_name),
					exc_info=e
				)
