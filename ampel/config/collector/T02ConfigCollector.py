#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : ampel/config/collector/T2RunConfigCollector.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 28.10.2019
# Last Modified Date: 28.10.2019
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import hashlib, json
from typing import Dict, Any
from ampel.db.DBUtils import DBUtils
from ampel.config.collector.TierConfigCollector import TierConfigCollector


class T2RunConfigCollector(TierConfigCollector):
	"""
	"""

	def add(self, arg: Dict[str, Any], dist_name: str = None) -> int:
		""" """

		hh =  DBUtils.b2_dict_hash(arg)

		# Check duplicated channel names
		if self.get(hh):
			if hashlib.sha256(bytes(json.dumps(self.get(hh), sort_keys=True), "utf-8")).digest() != \
				hashlib.sha256(bytes(json.dumps(arg, sort_keys=True), "utf-8")).digest():
				raise ValueError("Hash collision detected")
			if self.verbose:
				self.logger.verbose(
					f"-> Re-using T2 RunConfig hash: {hh}"
				)
			return hh

		if self.verbose:
			self.logger.verbose(
				f"-> Adding T2 RunConfig hash: {hh}"
			)

		self.__setitem__(hh, arg)
		return hh
