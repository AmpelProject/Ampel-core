#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-core/ampel/config/collector/T02ConfigCollector.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 28.10.2019
# Last Modified Date: 01.03.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import hashlib, json
from typing import Dict, Any, Optional, Union, List
from ampel.util.mappings import build_unsafe_short_dict_id
from ampel.config.collector.AbsDictConfigCollector import AbsDictConfigCollector
from ampel.log import VERBOSE


class T02ConfigCollector(AbsDictConfigCollector):
	"""
	Collect and hash T2 configurations contained in T0 processes
	(hence the name T02)
	Note: used hash algorithm is blake2.
	An additional hash collision test is performed using SHA256
	"""

	def add(self,
		arg: Union[Dict[str, Any], List[str]],
		file_name: Optional[str] = None,
		dist_name: Optional[str] = None
	) -> int:

		if not isinstance(arg, dict):
			raise ValueError("Illegal argument")

		# Uses blake2 hash algorithm
		hh = build_unsafe_short_dict_id(arg)

		if self.get(hh):

			# Build SHA256 hash
			hash1 = hashlib.sha256(bytes(json.dumps(self.get(hh), sort_keys=True), "utf-8")).digest()
			hash2 = hashlib.sha256(bytes(json.dumps(arg, sort_keys=True), "utf-8")).digest()

			if hash1 != hash2:
				raise ValueError("Hash collision detected")

			if self.verbose:
				self.logger.log(VERBOSE, f"Re-using T2 config hash: {hh}")

			return hh

		if self.verbose:
			self.logger.log(VERBOSE, f"Adding T2 config hash: {hh}")

		self.__setitem__(hh, arg)
		return hh
